#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import math
import re
import traceback
import unicodedata
import urllib.parse
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from migracao_html_text import strip_html_text

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


TARGET_TABLES = {
    "contratos": "dbo.migracao_contrato",
    "colaboradores": "dbo.migracao_colaborador",
    "integracoes": "dbo.migracao_treinamento",
    "operadores": "dbo.migracao_operador",
    "operadores_vinculos": "dbo.migracao_operador_alcada",
    "analises": "dbo.migracao_analise",
    "blacklist": "dbo.migracao_blacklist",
    "riscos": "dbo.migracao_risco",
    "procedimentos": "dbo.migracao_analise_procedimento",
}


@dataclass
class ImportStats:
    table_name: str
    parquet_name: str
    read_rows: int = 0
    prepared_rows: int = 0
    inserted_rows: int = 0
    failed_rows: int = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Motor de migração Parquet -> SQL Server")

    p.add_argument("--contratos", required=True)
    p.add_argument("--colaboradores", required=True)
    p.add_argument("--integracoes", required=False)
    p.add_argument("--operadores", required=False)
    p.add_argument("--operadores-vinculos", dest="operadores_vinculos", required=False)
    p.add_argument("--analises", required=True)
    p.add_argument("--blacklist", required=False)
    p.add_argument("--riscos", required=False)
    p.add_argument("--procedimentos", required=True)

    p.add_argument("--sqlserver-host", required=True)
    p.add_argument("--sqlserver-port", type=int, default=1433)
    p.add_argument("--sqlserver-db", required=True)
    p.add_argument("--sqlserver-user", required=True)
    p.add_argument("--sqlserver-password", required=True)
    p.add_argument("--sqlserver-driver", default="ODBC Driver 17 for SQL Server")

    p.add_argument("--company-id", type=int, required=True)
    p.add_argument(
        "--contratante-cnpj",
        default=None,
        help="Quando informado, limita a limpeza aos registros com contratante_cnpj igual ao valor.",
    )
    p.add_argument("--truncate", action="store_true")
    p.add_argument(
        "--only",
        choices=[
            "contratos",
            "colaboradores",
            "integracoes",
            "operadores",
            "operadores_vinculos",
            "analises",
            "blacklist",
            "riscos",
            "procedimentos",
        ],
        default=None,
    )

    p.add_argument("--chunksize", type=int, default=1000)
    p.add_argument("--row-fallback", action="store_true")
    p.add_argument("--error-dir", default="import_errors")

    return p.parse_args()


def build_engine(args: argparse.Namespace) -> Engine:
    def odbc_value(value):
        return "{" + str(value).replace("}", "}}") + "}"

    conn_str = (
        f"DRIVER={odbc_value(args.sqlserver_driver)};"
        f"SERVER={odbc_value(f'{args.sqlserver_host},{args.sqlserver_port}')};"
        f"DATABASE={odbc_value(args.sqlserver_db)};"
        f"UID={odbc_value(args.sqlserver_user)};"
        f"PWD={odbc_value(args.sqlserver_password)};"
        f"TrustServerCertificate=yes;"
    )
    params = urllib.parse.quote_plus(conn_str)

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        future=True,
        fast_executemany=True,
        pool_pre_ping=True,
    )


def normalize_null(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def clean_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(normalize_null)
    return df


def normalize_sqlserver_datetime(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    return s.dt.round("ms")


SQLSERVER_DATE_MIN = date(1, 1, 1)
SQLSERVER_DATE_MAX = date(9999, 12, 31)


def normalize_sqlserver_date_value(value: Any):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, datetime):
        result = value.date()
    elif isinstance(value, date):
        result = value
    elif isinstance(value, str):
        text_value = value.strip()
        if not text_value:
            return None
        try:
            result = date.fromisoformat(text_value[:10])
        except ValueError:
            return None
    else:
        try:
            timestamp = pd.Timestamp(value)
        except (OverflowError, TypeError, ValueError):
            return None
        if pd.isna(timestamp):
            return None
        result = timestamp.date()

    if result < SQLSERVER_DATE_MIN or result > SQLSERVER_DATE_MAX:
        return None
    return result


def coerce_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = normalize_sqlserver_datetime(df[col])
    return df


def coerce_date(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].map(normalize_sqlserver_date_value)
    return df


def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def to_bit_series(series: pd.Series) -> pd.Series:
    def conv(v: Any):
        if pd.isna(v):
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v != 0)
        s = str(v).strip().lower()
        if s in {"1", "true", "t", "yes", "y", "sim", "s", "active", "ativo"}:
            return 1
        if s in {"0", "false", "f", "no", "n", "nao", "não", "inactive", "inativo"}:
            return 0
        return None

    return series.map(conv)


def status_reprovado_series(series: pd.Series) -> pd.Series:
    def conv(v: Any):
        if pd.isna(v):
            return None
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip().lower()
        if s in {"reprovado", "failed", "fail", "rejected", "reject"}:
            return 1
        if s in {"aprovado", "approved", "pass", "passed", "ok"}:
            return 0
        return None

    return series.map(conv)


def truncate_table(engine: Engine, table_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table_name}"))


def delete_preserving_status_3(engine: Engine, table_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table_name} WHERE ISNULL(migracao_status, 0) <> 3"))


def normalize_cli_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def delete_by_contratante_cnpj(
    engine: Engine,
    table_name: str,
    contratante_cnpj: str,
    preserve_status_3: bool,
) -> None:
    table_meta = get_table_columns(engine, table_name)
    table_cols = set(table_meta["COLUMN_NAME"].tolist())
    if "contratante_cnpj" not in table_cols:
        print(
            f"[WARN] {table_name}: limpeza por contratante_cnpj ignorada; "
            "coluna contratante_cnpj não encontrada."
        )
        return

    where_parts = ["contratante_cnpj = :contratante_cnpj"]
    if preserve_status_3 and "migracao_status" in table_cols:
        where_parts.append("ISNULL(migracao_status, 0) <> 3")

    sql = text(f"DELETE FROM {table_name} WHERE {' AND '.join(where_parts)}")
    with engine.begin() as conn:
        result = conn.execute(sql, {"contratante_cnpj": contratante_cnpj})

    rowcount = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else None
    status_msg = " preservando migracao_status = 3" if preserve_status_3 and "migracao_status" in table_cols else ""
    if rowcount is None:
        print(
            f"[INFO] {table_name}: limpeza por contratante_cnpj={contratante_cnpj!r}{status_msg} finalizada."
        )
    else:
        print(
            f"[INFO] {table_name}: {rowcount} registro(s) removido(s) "
            f"com contratante_cnpj={contratante_cnpj!r}{status_msg}."
        )


def delete_contratos_status_diferente_3_duplicados_com_status3(engine: Engine, table_name: str) -> None:
    c0_cnpj = (
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
        "LTRIM(RTRIM(CAST(c0.contratante_cnpj AS NVARCHAR(64)))), "
        "N'.', N''), N'/', N''), N'-', N''), N' ', N''), NCHAR(160), N'')"
    )
    c3_cnpj = (
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
        "LTRIM(RTRIM(CAST(c3.contratante_cnpj AS NVARCHAR(64)))), "
        "N'.', N''), N'/', N''), N'-', N''), N' ', N''), NCHAR(160), N'')"
    )
    c0_contrato = (
        "LOWER(LTRIM(RTRIM("
        "REPLACE(REPLACE(REPLACE(CAST(c0.contrato_numero AS NVARCHAR(512)), "
        "NCHAR(160), N' '), N'  ', N' '), N'  ', N' ')"
        "))) COLLATE Latin1_General_100_CI_AI"
    )
    c3_contrato = (
        "LOWER(LTRIM(RTRIM("
        "REPLACE(REPLACE(REPLACE(CAST(c3.contrato_numero AS NVARCHAR(512)), "
        "NCHAR(160), N' '), N'  ', N' '), N'  ', N' ')"
        "))) COLLATE Latin1_General_100_CI_AI"
    )
    c0_contratada = "LTRIM(RTRIM(CAST(c0.contratada_identificador AS NVARCHAR(64))))"
    c3_contratada = "LTRIM(RTRIM(CAST(c3.contratada_identificador AS NVARCHAR(64))))"

    sql = text(
        f"""
        DELETE c0
        FROM {table_name} c0
        WHERE ISNULL(c0.migracao_status, 0) <> 3
          AND EXISTS (
              SELECT 1
              FROM {table_name} c3
              WHERE c3.migracao_status = 3
                AND {c0_cnpj} = {c3_cnpj}
                AND {c0_contrato} = {c3_contrato}
                AND (
                    (
                        TRY_CONVERT(BIGINT, c0.contratada_identificador) IS NOT NULL
                        AND TRY_CONVERT(BIGINT, c0.contratada_identificador)
                            = TRY_CONVERT(BIGINT, c3.contratada_identificador)
                    )
                    OR {c0_contratada} = {c3_contratada}
                )
          )
        """
    )

    with engine.begin() as conn:
        result = conn.execute(sql)

    rowcount = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else None
    if rowcount is None:
        print(f"[INFO] {table_name}: limpeza pós-importação de contratos duplicados finalizada.")
    else:
        print(
            f"[INFO] {table_name}: {rowcount} contrato(s) com migracao_status <> 3 removido(s) "
            "por duplicarem registro com migracao_status = 3."
        )


def delete_colaboradores_duplicados_motivo_demissao_nulo(engine: Engine, table_name: str) -> None:
    print(f"[INFO] {table_name}: iniciando limpeza de colaboradores duplicados...")
    table_meta = get_table_columns(engine, table_name)
    table_cols = set(table_meta["COLUMN_NAME"].tolist())
    motivo_col = None
    for candidate in ["colaborador_motivo_demissao", "colaborador_id_motivo_demissao"]:
        if candidate in table_cols:
            motivo_col = candidate
            break

    if motivo_col is None:
        print(
            f"[WARN] {table_name}: limpeza de duplicados não executada. "
            "Coluna de motivo de demissão não encontrada."
        )
        return

    sql = text(
        f"""
        WITH marcados AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        contrato_numero,
                        contratada_identificador,
                        colaborador_identificador,
                        competencia_tipo,
                        competencia_data
                    ORDER BY
                        CASE WHEN {motivo_col} IS NULL THEN 1 ELSE 0 END,
                        migracao_guid
                ) AS rn
            FROM {table_name}
        )
        DELETE FROM marcados
        WHERE rn > 1
          AND {motivo_col} IS NULL
        """
    )

    with engine.begin() as conn:
        result = conn.execute(sql)

    rowcount = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else None
    if rowcount is None:
        print(f"[INFO] {table_name}: limpeza pós-importação de colaboradores duplicados finalizada.")
    else:
        print(
            f"[INFO] {table_name}: {rowcount} colaborador(es) duplicado(s) com "
            f"{motivo_col} nulo removido(s)."
        )


def filter_existing_operadores(engine: Engine, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if (
        df.empty
        or "operador_identificador" not in df.columns
        or "contratante_cnpj" not in df.columns
    ):
        return df

    work = df.copy()

    work["operador_identificador"] = pd.to_numeric(
        work["operador_identificador"],
        errors="coerce",
    )

    work["contratante_cnpj"] = (
        work["contratante_cnpj"]
        .astype("string")
        .str.strip()
    )

    valid = work[
        work["operador_identificador"].notna()
        & work["contratante_cnpj"].notna()
        & (work["contratante_cnpj"] != "")
    ].copy()

    if valid.empty:
        return df

    valid["operador_identificador"] = valid["operador_identificador"].astype(int)

    # Remove duplicidade das chaves que serão consultadas
    keys = (
        valid[
            ["contratante_cnpj", "operador_identificador"]
        ]
        .drop_duplicates()
        .to_records(index=False)
        .tolist()
    )

    existing_keys: set[tuple[str, int]] = set()

    # Como cada chave usa 2 parâmetros, mantemos um lote menor
    batch_size = 500

    with engine.connect() as conn:
        for start in range(0, len(keys), batch_size):
            batch = keys[start:start + batch_size]

            conditions = []
            params = {}

            for i, (cnpj, operador_id) in enumerate(batch):
                conditions.append(
                    f"(contratante_cnpj = :cnpj{i} "
                    f"AND operador_identificador = :op{i})"
                )

                params[f"cnpj{i}"] = str(cnpj)
                params[f"op{i}"] = int(operador_id)

            sql = text(
                f"""
                SELECT
                    contratante_cnpj,
                    operador_identificador
                FROM {table_name}
                WHERE {" OR ".join(conditions)}
                """
            )

            rows = conn.execute(sql, params).fetchall()

            for row in rows:
                if row[0] is not None and row[1] is not None:
                    existing_keys.add(
                        (
                            str(row[0]).strip(),
                            int(row[1]),
                        )
                    )

    if not existing_keys:
        return df

    def registro_existe(row) -> bool:
        if pd.isna(row["operador_identificador"]):
            return False

        cnpj = row["contratante_cnpj"]

        if pd.isna(cnpj):
            return False

        cnpj = str(cnpj).strip()

        if not cnpj:
            return False

        return (
            cnpj,
            int(row["operador_identificador"]),
        ) in existing_keys

    exists_mask = work.apply(registro_existe, axis=1)
    keep_mask = ~exists_mask

    skipped = int(exists_mask.sum())

    if skipped:
        print(
            f"[INFO] {table_name}: {skipped} registro(s) ignorado(s) "
            f"por contratante_cnpj + operador_identificador já existente."
        )

    return df.loc[keep_mask].copy()


def filter_existing_operadores_vinculos(engine: Engine, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    required = {"contratada_identificador", "operador_identificador"}
    if df.empty or not required.issubset(df.columns):
        return df

    pairs_df = pd.DataFrame({
        "contratada_identificador": pd.to_numeric(df["contratada_identificador"], errors="coerce"),
        "operador_identificador": pd.to_numeric(df["operador_identificador"], errors="coerce"),
    }).dropna()

    if pairs_df.empty:
        return df

    pairs_df = pairs_df.astype({"contratada_identificador": int, "operador_identificador": int}).drop_duplicates()
    existing_pairs: set[tuple[int, int]] = set()
    batch_size = 1000

    with engine.connect() as conn:
        records = pairs_df.to_dict(orient="records")
        for start in range(0, len(records), batch_size):
            batch = records[start:start + batch_size]
            conditions = []
            params: dict[str, int] = {}
            for i, rec in enumerate(batch):
                conditions.append(f"(contratada_identificador = :c{i} AND operador_identificador = :o{i})")
                params[f"c{i}"] = int(rec["contratada_identificador"])
                params[f"o{i}"] = int(rec["operador_identificador"])

            sql = text(
                f"SELECT contratada_identificador, operador_identificador "
                f"FROM {table_name} WHERE " + " OR ".join(conditions)
            )
            rows = conn.execute(sql, params).fetchall()
            existing_pairs.update(
                (int(r[0]), int(r[1]))
                for r in rows
                if r[0] is not None and r[1] is not None
            )

    if not existing_pairs:
        return df

    cand_contratada = pd.to_numeric(df["contratada_identificador"], errors="coerce")
    cand_operador = pd.to_numeric(df["operador_identificador"], errors="coerce")

    pair_series = list(
        zip(
            cand_contratada.fillna(-1).astype(int, errors="ignore"),
            cand_operador.fillna(-1).astype(int, errors="ignore"),
        )
    )
    keep_mask = pd.Series([pair not in existing_pairs for pair in pair_series], index=df.index)
    skipped = int((~keep_mask).sum())
    if skipped:
        print(
            f"[INFO] {table_name}: {skipped} registro(s) ignorado(s) por chave composta "
            f"(contratada_identificador, operador_identificador) já existente."
        )
    return df.loc[keep_mask].copy()


def normalize_text_key(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text_value = str(value).replace("\xa0", " ")
    text_value = re.sub(r"\s+", " ", text_value).strip()
    if not text_value:
        return None

    text_value = unicodedata.normalize("NFKD", text_value)
    text_value = "".join(ch for ch in text_value if not unicodedata.combining(ch))
    return text_value.casefold()


def normalize_cnpj_key(value: Any) -> str | None:
    text_value = normalize_text_key(value)
    if text_value is None:
        return None

    digits = re.sub(r"\D", "", text_value)
    return digits or text_value


def normalize_integral_key(value: Any) -> str | None:
    if pd.isna(value):
        return None

    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric_value) and math.isfinite(float(numeric_value)):
        numeric_float = float(numeric_value)
        if numeric_float.is_integer():
            return str(int(numeric_float))
        return str(numeric_value)

    return normalize_text_key(value)


def filter_existing_contratos(engine: Engine, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    key_cols = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
    ]
    if df.empty or not set(key_cols).issubset(df.columns):
        return df

    keys_df = pd.DataFrame(index=df.index)
    keys_df["contratante_cnpj"] = df["contratante_cnpj"].map(normalize_cnpj_key)
    keys_df["contrato_numero"] = df["contrato_numero"].map(normalize_text_key)
    keys_df["contratada_identificador"] = df["contratada_identificador"].map(normalize_integral_key)

    complete_keys = keys_df.dropna().copy()
    if complete_keys.empty:
        return df

    incoming_duplicate_mask = complete_keys.duplicated(keep="first")
    keep_incoming_mask = pd.Series(True, index=df.index)
    if incoming_duplicate_mask.any():
        duplicate_indexes = complete_keys.index[incoming_duplicate_mask]
        keep_incoming_mask.loc[duplicate_indexes] = False
        skipped_incoming = int(incoming_duplicate_mask.sum())
        print(
            f"[INFO] {table_name}: {skipped_incoming} registro(s) ignorado(s) por chave composta "
            "(contratante_cnpj, contrato_numero, contratada_identificador) duplicada no arquivo de entrada."
        )

    existing_keys: set[tuple[Any, ...]] = set()

    with engine.connect() as conn:
        sql = text(
            "SELECT DISTINCT "
            "contratante_cnpj, "
            "contrato_numero, "
            "contratada_identificador "
            f"FROM {table_name} "
            "WHERE contratante_cnpj IS NOT NULL "
            "AND contrato_numero IS NOT NULL "
            "AND contratada_identificador IS NOT NULL"
        )
        rows = conn.execute(sql).fetchall()
        existing_keys.update(
            key
            for key in (
                (
                    normalize_cnpj_key(r[0]),
                    normalize_text_key(r[1]),
                    normalize_integral_key(r[2]),
                )
                for r in rows
            )
            if all(v is not None for v in key)
        )

    print(f"[INFO] {table_name}: {len(existing_keys)} chave(s) de contrato já existente(s) carregada(s) da base final.")

    candidate_keys = list(
        zip(
            keys_df["contratante_cnpj"].astype(object),
            keys_df["contrato_numero"].astype(object),
            keys_df["contratada_identificador"].astype(object),
        )
    )
    keep_existing_mask = pd.Series([key not in existing_keys for key in candidate_keys], index=df.index)
    keep_mask = keep_incoming_mask & keep_existing_mask
    skipped = int((~keep_existing_mask).sum())
    if skipped:
        print(
            f"[INFO] {table_name}: {skipped} registro(s) ignorado(s) por chave composta "
            "(contratante_cnpj, contrato_numero, contratada_identificador) já existente."
        )
    return df.loc[keep_mask].copy()


def filter_existing_colaboradores(engine: Engine, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    key_cols = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "colaborador_identificador",
        "competencia_data",
        "competencia_tipo",
    ]
    if df.empty or not set(key_cols).issubset(df.columns):
        return df

    keys_df = pd.DataFrame(index=df.index)
    keys_df["contratante_cnpj"] = df["contratante_cnpj"].map(normalize_cnpj_key)
    keys_df["contrato_numero"] = df["contrato_numero"].map(normalize_text_key)
    keys_df["contratada_identificador"] = df["contratada_identificador"].map(normalize_integral_key)
    keys_df["colaborador_identificador"] = df["colaborador_identificador"].map(normalize_integral_key)
    keys_df["competencia_data"] = pd.to_datetime(df["competencia_data"], errors="coerce").dt.date
    keys_df["competencia_tipo"] = df["competencia_tipo"].map(normalize_integral_key)

    complete_keys = keys_df.dropna().copy()
    if complete_keys.empty:
        return df

    complete_keys = complete_keys.drop_duplicates()

    existing_keys: set[tuple[Any, ...]] = set()

    with engine.connect() as conn:
        sql = text(
            "SELECT DISTINCT "
            "contratante_cnpj, "
            "contrato_numero, "
            "contratada_identificador, "
            "colaborador_identificador, "
            "CAST(competencia_data AS date) AS competencia_data, "
            "competencia_tipo "
            f"FROM {table_name} "
            "WHERE contratante_cnpj IS NOT NULL "
            "AND contrato_numero IS NOT NULL "
            "AND contratada_identificador IS NOT NULL "
            "AND colaborador_identificador IS NOT NULL "
            "AND competencia_data IS NOT NULL "
            "AND competencia_tipo IS NOT NULL"
        )
        rows = conn.execute(sql).fetchall()
        existing_keys.update(
            key
            for key in (
                (
                    normalize_cnpj_key(r[0]),
                    normalize_text_key(r[1]),
                    normalize_integral_key(r[2]),
                    normalize_integral_key(r[3]),
                    pd.to_datetime(r[4]).date(),
                    normalize_integral_key(r[5]),
                )
                for r in rows
            )
            if all(v is not None for v in key)
        )

    if not existing_keys:
        return df

    candidate_keys = list(
        zip(
            keys_df["contratante_cnpj"].astype(object),
            keys_df["contrato_numero"].astype(object),
            keys_df["contratada_identificador"].astype(object),
            keys_df["colaborador_identificador"].astype(object),
            keys_df["competencia_data"],
            keys_df["competencia_tipo"].astype(object),
        )
    )
    keep_mask = pd.Series([key not in existing_keys for key in candidate_keys], index=df.index)
    skipped = int((~keep_mask).sum())
    if skipped:
        print(
            f"[INFO] {table_name}: {skipped} registro(s) ignorado(s) por chave composta "
            "(contratante_cnpj, contrato_numero, contratada_identificador, "
            "colaborador_identificador, competencia_data, competencia_tipo) já existente."
        )
    return df.loc[keep_mask].copy()


def get_table_columns(engine: Engine, table_name: str) -> pd.DataFrame:
    schema, table = table_name.split(".", 1)

    sql = text(
        """
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.DATETIME_PRECISION,
            c.IS_NULLABLE,
            c.ORDINAL_POSITION,
            COLUMNPROPERTY(
                OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME),
                c.COLUMN_NAME,
                'IsIdentity'
            ) AS IS_IDENTITY,
            COLUMNPROPERTY(
                OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME),
                c.COLUMN_NAME,
                'IsComputed'
            ) AS IS_COMPUTED
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = :schema
          AND c.TABLE_NAME = :table
        ORDER BY c.ORDINAL_POSITION
        """
    )

    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"schema": schema, "table": table})


def truncate_strings_to_table_limit(df: pd.DataFrame, table_meta: pd.DataFrame) -> pd.DataFrame:
    for _, row in table_meta.iterrows():
        col = row["COLUMN_NAME"]
        max_len = row["CHARACTER_MAXIMUM_LENGTH"]

        if col not in df.columns:
            continue
        if pd.isna(max_len) or max_len in (-1, 0):
            continue

        max_len = int(max_len)

        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(lambda x: x[:max_len] if isinstance(x, str) else x)

    return df


def sanitize_temporal_columns_by_table(df: pd.DataFrame, table_meta: pd.DataFrame) -> pd.DataFrame:
    type_map = {}
    for _, row in table_meta.iterrows():
        col = row["COLUMN_NAME"]
        typ = str(row["DATA_TYPE"]).lower()
        type_map[col] = typ

    for col in df.columns:
        if col not in type_map:
            continue

        typ = type_map[col]

        if typ == "date":
            df[col] = df[col].map(normalize_sqlserver_date_value)

        elif typ == "datetime":
            s = pd.to_datetime(df[col], errors="coerce").dt.round("ms")
            min_dt = pd.Timestamp("1753-01-01 00:00:00")
            max_dt = pd.Timestamp("2262-04-11 23:47:16.854")
            s = s.where((s.isna()) | ((s >= min_dt) & (s <= max_dt)))
            df[col] = s

        elif typ == "datetime2":
            s = pd.to_datetime(df[col], errors="coerce")
            min_dt = pd.Timestamp("1677-09-22 00:00:00")
            max_dt = pd.Timestamp("2262-04-11 23:47:16.854")
            s = s.where((s.isna()) | ((s >= min_dt) & (s <= max_dt)))
            df[col] = s

        elif typ == "smalldatetime":
            s = pd.to_datetime(df[col], errors="coerce").dt.round("min")
            min_dt = pd.Timestamp("1900-01-01 00:00:00")
            max_dt = pd.Timestamp("2079-06-06 23:59:00")
            s = s.where((s.isna()) | ((s >= min_dt) & (s <= max_dt)))
            df[col] = s

        elif typ == "time":
            s = pd.to_datetime(df[col], errors="coerce")
            df[col] = s.dt.time

    return df


def validate_temporal_columns_by_table(df: pd.DataFrame, table_name: str, table_meta: pd.DataFrame) -> None:
    temporal_types = {"date", "datetime", "datetime2", "smalldatetime", "time"}
    meta = table_meta[table_meta["DATA_TYPE"].str.lower().isin(temporal_types)].copy()

    for _, row in meta.iterrows():
        col = row["COLUMN_NAME"]
        typ = str(row["DATA_TYPE"]).lower()

        if col not in df.columns:
            continue

        if typ == "date":
            original_not_null = df[col].notna()
            normalized = df[col].map(normalize_sqlserver_date_value)
            bad = df[original_not_null & normalized.isna()]
        elif typ == "datetime":
            s = pd.to_datetime(df[col], errors="coerce")
            bad = df[s.notna() & ((s < pd.Timestamp("1753-01-01")) | (s > pd.Timestamp("2262-04-11 23:47:16.854")))]
        elif typ == "datetime2":
            s = pd.to_datetime(df[col], errors="coerce")
            bad = df[s.notna() & ((s < pd.Timestamp("1677-09-22")) | (s > pd.Timestamp("2262-04-11 23:47:16.854")))]
        elif typ == "smalldatetime":
            s = pd.to_datetime(df[col], errors="coerce")
            bad = df[s.notna() & ((s < pd.Timestamp("1900-01-01")) | (s > pd.Timestamp("2079-06-06 23:59:00")))]
        else:
            bad = pd.DataFrame()

        if not bad.empty:
            print(f"[ERRO-DATA] {table_name}.{col} possui {len(bad)} valor(es) fora do range de {typ}")
            print(bad[[col]].head(20).to_dict(orient="records"))


def prepare_for_insert(df: pd.DataFrame, table_meta: pd.DataFrame) -> pd.DataFrame:
    table_cols = table_meta["COLUMN_NAME"].tolist()
    excluded = table_meta.loc[
        (table_meta["IS_IDENTITY"] == 1) | (table_meta["IS_COMPUTED"] == 1),
        "COLUMN_NAME",
    ].tolist()

    usable_cols = [c for c in df.columns if c in table_cols and c not in excluded]
    out = df[usable_cols].copy()
    out = out.map(normalize_null)
    out = clean_object_columns(out)
    out = sanitize_temporal_columns_by_table(out, table_meta)
    out = truncate_strings_to_table_limit(out, table_meta)
    return out


def print_diagnostics(df: pd.DataFrame, table_name: str, table_meta: pd.DataFrame) -> None:
    table_cols = table_meta["COLUMN_NAME"].tolist()
    df_cols = df.columns.tolist()

    missing_in_df = [c for c in table_cols if c not in df_cols]
    extra_in_df = [c for c in df_cols if c not in table_cols]

    print(f"\n[DIAG] {table_name}")
    print(f"[DIAG] Colunas dataframe: {len(df_cols)} | Colunas tabela: {len(table_cols)}")

    if missing_in_df:
        print(f"[DIAG] Faltando no DataFrame: {missing_in_df}")
    if extra_in_df:
        print(f"[DIAG] Sobrando no DataFrame: {extra_in_df}")

    print("[DIAG] Tipos DataFrame:")
    print(df.dtypes)

    print("[DIAG] Nulos por coluna:")
    print(df.isnull().sum())


def write_error_rows(df_errors: pd.DataFrame, error_path: Path) -> None:
    error_path.parent.mkdir(parents=True, exist_ok=True)
    df_errors.to_csv(error_path, index=False, encoding="utf-8-sig")


def load_pyarrow_parquet():
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Importação em lotes de analises.parquet requer o pacote pyarrow. "
            "Instale pyarrow no ambiente que executa este script."
        ) from exc

    return pq


def iter_parquet_batches(parquet_path: Path, batch_size: int):
    pq = load_pyarrow_parquet()
    parquet_file = pq.ParquetFile(parquet_path)
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        yield batch.to_pandas()


def insert_dataframe(
    engine: Engine,
    table_name: str,
    parquet_name: str,
    df: pd.DataFrame,
    chunksize: int,
    row_fallback: bool,
    error_dir: Path,
    table_meta: pd.DataFrame | None = None,
    show_diagnostics: bool = True,
) -> ImportStats:
    stats = ImportStats(table_name=table_name, parquet_name=parquet_name, read_rows=len(df), prepared_rows=len(df))

    if df.empty:
        print(f"[INFO] {table_name}: dataframe vazio, nada a inserir.")
        return stats

    if table_meta is None:
        table_meta = get_table_columns(engine, table_name)
    if show_diagnostics:
        print_diagnostics(df, table_name, table_meta)

    print(f"[INFO] {table_name}: iniciando prepare_for_insert...")
    df_insert = prepare_for_insert(df, table_meta)
    print(f"[INFO] {table_name}: prepare_for_insert finalizado com {len(df_insert)} linhas")

    print(f"[INFO] {table_name}: iniciando validate_temporal_columns_by_table...")
    validate_temporal_columns_by_table(df_insert, table_name, table_meta)
    print(f"[INFO] {table_name}: validação temporal finalizada")

    schema, table = table_name.split(".", 1)

    effective_chunksize = chunksize
    if table_name == "dbo.migracao_analise":
        effective_chunksize = min(chunksize, 1000)

    try:
        total = len(df_insert)
        print(f"[INFO] {table_name}: iniciando to_sql com chunksize={effective_chunksize}...")

        for start in range(0, total, effective_chunksize):
            end = min(start + effective_chunksize, total)
            chunk = df_insert.iloc[start:end].copy()

            print(f"[INFO] {table_name}: inserindo linhas {start + 1} até {end} de {total}...")

            chunk.to_sql(
                name=table,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=len(chunk),
            )

        stats.inserted_rows = len(df_insert)
        print(f"[OK] {table_name}: {stats.inserted_rows} linhas inseridas.")
        return stats

    except Exception as batch_error:
        print(f"[ERRO] Batch falhou em {table_name}: {batch_error}")

        if not row_fallback:
            raise

        print(f"[INFO] Tentando fallback linha a linha em {table_name}...")
        ok_rows = 0
        error_rows: list[dict[str, Any]] = []

        for idx in range(len(df_insert)):
            row_df = df_insert.iloc[[idx]].copy()
            try:
                row_df.to_sql(
                    name=table,
                    con=engine,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    chunksize=1,
                )
                ok_rows += 1
            except Exception as row_error:
                row_data = row_df.iloc[0].to_dict()
                row_data["_error"] = str(row_error)
                row_data["_trace"] = traceback.format_exc(limit=1)
                error_rows.append(row_data)

        stats.inserted_rows = ok_rows
        stats.failed_rows = len(error_rows)

        if error_rows:
            error_file = error_dir / f"{table.replace('.', '_')}_errors.csv"
            write_error_rows(pd.DataFrame(error_rows), error_file)
            print(f"[WARN] {table_name}: {stats.failed_rows} linhas com erro. Arquivo: {error_file}")

        print(f"[OK] {table_name}: {stats.inserted_rows} linhas inseridas no fallback.")
        return stats


# =========================
# MAPEAMENTOS
# =========================

def map_contratos(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None

    out["contrato_grupo_area"] = src["contrato_grupo_area"] if "contrato_grupo_area" in src.columns else None
    out["contrato_area"] = src["contrato_area"] if "contrato_area" in src.columns else None
    out["contrato_grupo_area"] = out["contrato_grupo_area"].replace(r"^\s*$", pd.NA, regex=True).fillna("MF - MOM")
    out["contrato_area"] = out["contrato_area"].replace(r"^\s*$", pd.NA, regex=True).fillna("HSE")
    out["contrato_familia"] = src["contrato_familia"] if "contrato_familia" in src.columns else None
    out["contrato_subfamilia"] = src["contrato_subfamilia"] if "contrato_subfamilia" in src.columns else None
    out["contrato_unidades"] = src["contrato_unidades"] if "contrato_unidades" in src.columns else None
    out["contrato_numero"] = src["contrato_numero"] if "contrato_numero" in src.columns else None
    out["contrato_data_inicio_vigencia"] = src["contrato_data_inicio_vigencia"] if "contrato_data_inicio_vigencia" in src.columns else None
    out["contrato_data_fim_vigencia"] = src["contrato_data_fim_vigencia"] if "contrato_data_fim_vigencia" in src.columns else None
    out["contrato_servico_mobilizacao"] = src["contrato_servico_mobilizacao"] if "contrato_servico_mobilizacao" in src.columns else None
    out["contrato_servico_trabalhista"] = src["contrato_servico_trabalhista"] if "contrato_servico_trabalhista" in src.columns else None
    out["contrato_servico_veiculo"] = src["contrato_servico_veiculo"] if "contrato_servico_veiculo" in src.columns else None
    out["contrato_servico_ambiental"] = src["contrato_servico_ambiental"] if "contrato_servico_ambiental" in src.columns else None
    out["contrato_numero_ordem_servico"] = src["contrato_numero_ordem_servico"] if "contrato_numero_ordem_servico" in src.columns else None
    out["contrato_numero_contrato_juridico"] = src["contrato_numero_contrato_juridico"] if "contrato_numero_contrato_juridico" in src.columns else None
    out["contrato_local_servico"] = src["contrato_local_servico"] if "contrato_local_servico" in src.columns else None
    out["contrato_cei"] = src["contrato_cei"] if "contrato_cei" in src.columns else None
    out["contrato_valor_servico"] = src["contrato_valor_servico"] if "contrato_valor_servico" in src.columns else None
    out["contrato_valor_equipamento"] = src["contrato_valor_equipamento"] if "contrato_valor_equipamento" in src.columns else None
    out["contrato_valor_material"] = src["contrato_valor_material"] if "contrato_valor_material" in src.columns else None
    out["contrato_valor_reembolso"] = src["contrato_valor_reembolso"] if "contrato_valor_reembolso" in src.columns else None
    out["contrato_utiliza_mao_obra"] = src["contrato_utiliza_mao_obra"] if "contrato_utiliza_mao_obra" in src.columns else None
    out["contrato_classificacao_objeto_contratual"] = src["contrato_classificacao_objeto_contratual"] if "contrato_classificacao_objeto_contratual" in src.columns else None
    out["contrato_objeto_contratual"] = src["contrato_objeto_contratual"] if "contrato_objeto_contratual" in src.columns else None
    out["contrato_observacoes_gerais"] = src["contrato_observacoes_gerais"] if "contrato_observacoes_gerais" in src.columns else None
    out["contrato_descricao"] = src["contrato_descricao"] if "contrato_descricao" in src.columns else None
    out["contrato_custeio_alimentacao"] = src["contrato_custeio_alimentacao"] if "contrato_custeio_alimentacao" in src.columns else None
    out["contrato_custeio_abastecimento"] = src["contrato_custeio_abastecimento"] if "contrato_custeio_abastecimento" in src.columns else None
    out["contrato_responsavel_identificador"] = src["contrato_responsavel_identificador"] if "contrato_responsavel_identificador" in src.columns else None

    out["contratada_identificador"] = src["contratada_identificador"] if "contratada_identificador" in src.columns else None
    out["contratada_cnpj"] = src["contratada_cnpj"] if "contratada_cnpj" in src.columns else None
    out["contratada_razao_social"] = src["contratada_razao_social"] if "contratada_razao_social" in src.columns else None
    out["contratada_fantasia"] = src["contratada_fantasia"] if "contratada_fantasia" in src.columns else None
    out["contratada_tipo_pessoa"] = src["contratada_tipo_pessoa"] if "contratada_tipo_pessoa" in src.columns else None
    out["contratada_tipo"] = src["contratada_tipo"] if "contratada_tipo" in src.columns else None
    out["contratada_data_inicio_trabalhista"] = src["contratada_data_inicio_trabalhista"] if "contratada_data_inicio_trabalhista" in src.columns else None
    out["contratada_data_fim_trabalhista"] = src["contratada_data_fim_trabalhista"] if "contratada_data_fim_trabalhista" in src.columns else None
    out["contratada_data_inicio_mobilizacao"] = src["contratada_data_inicio_mobilizacao"] if "contratada_data_inicio_mobilizacao" in src.columns else None
    out["contratada_data_fim_mobilizacao"] = src["contratada_data_fim_mobilizacao"] if "contratada_data_fim_mobilizacao" in src.columns else None
    out["contratada_inscricao_estadual"] = src["contratada_inscricao_estadual"] if "contratada_inscricao_estadual" in src.columns else None
    out["contratada_inscricao_municipal"] = src["contratada_inscricao_municipal"] if "contratada_inscricao_municipal" in src.columns else None
    out["contratada_endereco"] = src["contratada_endereco"] if "contratada_endereco" in src.columns else None
    out["contratada_numero"] = src["contratada_numero"] if "contratada_numero" in src.columns else None
    out["contratada_complemento"] = src["contratada_complemento"] if "contratada_complemento" in src.columns else None
    out["contratada_bairro"] = src["contratada_bairro"] if "contratada_bairro" in src.columns else None
    out["contratada_cidade"] = src["contratada_cidade"] if "contratada_cidade" in src.columns else None
    out["contratada_uf"] = src["contratada_uf"] if "contratada_uf" in src.columns else None
    out["contratada_cep"] = src["contratada_cep"] if "contratada_cep" in src.columns else None
    out["contratada_fone"] = src["contratada_fone"] if "contratada_fone" in src.columns else None
    out["contratada_fax"] = src["contratada_fax"] if "contratada_fax" in src.columns else None
    out["contratada_responsavel_identificador"] = src["contratada_responsavel_identificador"] if "contratada_responsavel_identificador" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_grupo_area_id"] = None
    out["migracao_area_id"] = None
    out["migracao_contrato_id"] = None
    out["migracao_contratada_id"] = None
    out["migracao_ctc_id"] = None

    out = coerce_date(out, [
        "contrato_data_inicio_vigencia",
        "contrato_data_fim_vigencia",
        "contratada_data_inicio_trabalhista",
        "contratada_data_fim_trabalhista",
        "contratada_data_inicio_mobilizacao",
        "contratada_data_fim_mobilizacao",
    ])

    out = coerce_datetime(out, [
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "contrato_servico_mobilizacao",
        "contrato_servico_trabalhista",
        "contrato_servico_veiculo",
        "contrato_servico_ambiental",
        "contrato_valor_servico",
        "contrato_valor_equipamento",
        "contrato_valor_material",
        "contrato_valor_reembolso",
        "contrato_utiliza_mao_obra",
        "contrato_classificacao_objeto_contratual",
        "contrato_responsavel_identificador",
        "contratada_identificador",
        "contratada_numero",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_grupo_area_id",
        "migracao_area_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_ctc_id",
    ])

    for col in [
        "contrato_servico_mobilizacao",
        "contrato_servico_trabalhista",
        "contrato_servico_veiculo",
        "contrato_servico_ambiental",
        "contrato_utiliza_mao_obra",
    ]:
        out[col] = to_bit_series(out[col])

    ordered = [
        "contratante_cnpj",
        "contrato_grupo_area",
        "contrato_area",
        "contrato_familia",
        "contrato_subfamilia",
        "contrato_unidades",
        "contrato_numero",
        "contrato_data_inicio_vigencia",
        "contrato_data_fim_vigencia",
        "contrato_servico_mobilizacao",
        "contrato_servico_trabalhista",
        "contrato_servico_veiculo",
        "contrato_servico_ambiental",
        "contrato_numero_ordem_servico",
        "contrato_numero_contrato_juridico",
        "contrato_local_servico",
        "contrato_cei",
        "contrato_valor_servico",
        "contrato_valor_equipamento",
        "contrato_valor_material",
        "contrato_valor_reembolso",
        "contrato_utiliza_mao_obra",
        "contrato_classificacao_objeto_contratual",
        "contrato_objeto_contratual",
        "contrato_observacoes_gerais",
        "contrato_descricao",
        "contrato_custeio_alimentacao",
        "contrato_custeio_abastecimento",
        "contrato_responsavel_identificador",
        "contratada_identificador",
        "contratada_cnpj",
        "contratada_razao_social",
        "contratada_fantasia",
        "contratada_tipo_pessoa",
        "contratada_tipo",
        "contratada_data_inicio_trabalhista",
        "contratada_data_fim_trabalhista",
        "contratada_data_inicio_mobilizacao",
        "contratada_data_fim_mobilizacao",
        "contratada_inscricao_estadual",
        "contratada_inscricao_municipal",
        "contratada_endereco",
        "contratada_numero",
        "contratada_complemento",
        "contratada_bairro",
        "contratada_cidade",
        "contratada_uf",
        "contratada_cep",
        "contratada_fone",
        "contratada_fax",
        "contratada_responsavel_identificador",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_grupo_area_id",
        "migracao_area_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_ctc_id",
    ]

    return out[ordered]


def map_colaboradores(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    out = df.copy()

    rename_candidates = {
        "colaborador_id_motivo_admissao": "colaborador_motivo_admissao",
        "colaborador_id_motivo_demissao": "colaborador_motivo_demissao",
    }

    for src, dst in rename_candidates.items():
        if src in out.columns and dst not in out.columns:
            out = out.rename(columns={src: dst})

    expected_cols = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "competencia_data",
        "competencia_tipo",
        "colaborador_identificador",
        "colaborador_matricula",
        "colaborador_cpf",
        "colaborador_rg",
        "colaborador_passaporte",
        "colaborador_pis",
        "colaborador_nome",
        "colaborador_sexo",
        "colaborador_estrangeiro",
        "colaborador_data_nascimento",
        "colaborador_email",
        "colaborador_observacao",
        "colaborador_funcao",
        "colaborador_salario",
        "colaborador_local_servicos",
        "colaborador_exposicao_risco",
        "colaborador_data_inicio_servico",
        "colaborador_data_termino_contrato",
        "colaborador_motivo_admissao",
        "colaborador_data_admissao",
        "colaborador_motivo_demissao",
        "colaborador_data_desligamento",
        "colaborador_ferias",
        "colaborador_id_classificacao",
        "colaborador_refeicao_custeada",
        "colaborador_tipo_prestador",
        "colaborador_amostragem",
        "colaborador_possui_pendencia",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_competencia_id",
        "migracao_colaborador_id",
        "migracao_lcc_id",
    ]

    for col in expected_cols:
        if col not in out.columns:
            out[col] = None

    if "colaborador_estrangeiro" in out.columns:
        vazio = out["colaborador_estrangeiro"].isna()
        if "colaborador_passaporte" in out.columns:
            out.loc[vazio, "colaborador_estrangeiro"] = (
                out.loc[vazio, "colaborador_passaporte"].astype("string").str.strip().fillna("") != ""
            ).astype(int)

    out = coerce_date(out, [
        "competencia_data",
        "colaborador_data_nascimento",
        "colaborador_data_inicio_servico",
        "colaborador_data_termino_contrato",
        "colaborador_data_admissao",
        "colaborador_data_desligamento",
    ])

    out = coerce_datetime(out, [
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "competencia_tipo",
        "colaborador_salario",
        "colaborador_motivo_admissao",
        "colaborador_motivo_demissao",
        "colaborador_id_classificacao",
        "colaborador_tipo_prestador",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_competencia_id",
        "migracao_colaborador_id",
        "migracao_lcc_id",
    ])

    for col in [
        "colaborador_estrangeiro",
        "colaborador_ferias",
        "colaborador_refeicao_custeada",
        "colaborador_amostragem",
        "colaborador_possui_pendencia",
    ]:
        if col in out.columns:
            out[col] = to_bit_series(out[col])

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_contrato_id"] = None
    out["migracao_contratada_id"] = None
    out["migracao_competencia_id"] = None
    out["migracao_colaborador_id"] = None
    out["migracao_lcc_id"] = None

    ordered = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "competencia_data",
        "competencia_tipo",
        "colaborador_identificador",
        "colaborador_matricula",
        "colaborador_cpf",
        "colaborador_rg",
        "colaborador_passaporte",
        "colaborador_pis",
        "colaborador_nome",
        "colaborador_sexo",
        "colaborador_estrangeiro",
        "colaborador_data_nascimento",
        "colaborador_email",
        "colaborador_observacao",
        "colaborador_funcao",
        "colaborador_salario",
        "colaborador_local_servicos",
        "colaborador_exposicao_risco",
        "colaborador_data_inicio_servico",
        "colaborador_data_termino_contrato",
        "colaborador_motivo_admissao",
        "colaborador_data_admissao",
        "colaborador_motivo_demissao",
        "colaborador_data_desligamento",
        "colaborador_ferias",
        "colaborador_id_classificacao",
        "colaborador_refeicao_custeada",
        "colaborador_tipo_prestador",
        "colaborador_amostragem",
        "colaborador_possui_pendencia",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_competencia_id",
        "migracao_colaborador_id",
        "migracao_lcc_id",
    ]

    return out[ordered]


def map_integracoes(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None
    out["contrato_numero"] = src["contrato_numero"] if "contrato_numero" in src.columns else None
    out["contratada_identificador"] = src["contratada_identificador"] if "contratada_identificador" in src.columns else None
    out["colaborador_identificador"] = src["colaborador_identificador"] if "colaborador_identificador" in src.columns else None

    out["treinamento_apelido"] = src["treinamento_apelido"] if "treinamento_apelido" in src.columns else None
    out["treinamento_descricao"] = src["treinamento_descricao"] if "treinamento_descricao" in src.columns else None
    out["treinamento_endereco"] = src["treinamento_endereco"] if "treinamento_endereco" in src.columns else None
    out["treinamento_prazo_integracao"] = src["treinamento_prazo_integracao"] if "treinamento_prazo_integracao" in src.columns else None
    out["treinamento_horario_inicial"] = src["treinamento_horario_inicial"] if "treinamento_horario_inicial" in src.columns else None
    out["treinamento_horario_final"] = src["treinamento_horario_final"] if "treinamento_horario_final" in src.columns else None
    out["treinamento_dia_semana"] = src["treinamento_dia_semana"] if "treinamento_dia_semana" in src.columns else None

    out["turma_data"] = src["turma_data"] if "turma_data" in src.columns else None
    out["turma_descricao"] = src["turma_descricao"] if "turma_descricao" in src.columns else None
    out["turma_quantidade_colaborador"] = src["turma_quantidade_colaborador"] if "turma_quantidade_colaborador" in src.columns else None

    out["colaborador_aprovado"] = src["colaborador_aprovado"] if "colaborador_aprovado" in src.columns else None
    out["colaborador_data_aprovacao"] = src["colaborador_data_aprovacao"] if "colaborador_data_aprovacao" in src.columns else None
    out["colaborador_nota"] = src["colaborador_nota"] if "colaborador_nota" in src.columns else None
    out["colaborador_motivo_reprovacao"] = src["colaborador_motivo_reprovacao"] if "colaborador_motivo_reprovacao" in src.columns else None
    out["aprovacao_autor_identificador"] = src["aprovacao_autor_identificador"] if "aprovacao_autor_identificador" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_treinamento_id"] = None
    out["migracao_turma_id"] = None
    out["migracao_turma_colaborador_id"] = None
    out["migracao_lcc_id"] = None
    out["migracao_operador_aprovacao_id"] = None

    out = coerce_date(out, [
        "turma_data",
    ])

    out = coerce_datetime(out, [
        "colaborador_data_aprovacao",
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "treinamento_prazo_integracao",
        "turma_quantidade_colaborador",
        "colaborador_motivo_reprovacao",
        "migracao_status",
        "migracao_treinamento_id",
        "migracao_turma_id",
        "migracao_turma_colaborador_id",
        "migracao_lcc_id",
        "migracao_operador_aprovacao_id",
    ])

    out["colaborador_aprovado"] = to_bit_series(out["colaborador_aprovado"])

    ordered = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "colaborador_identificador",
        "treinamento_apelido",
        "treinamento_descricao",
        "treinamento_endereco",
        "treinamento_prazo_integracao",
        "treinamento_horario_inicial",
        "treinamento_horario_final",
        "treinamento_dia_semana",
        "turma_data",
        "turma_descricao",
        "turma_quantidade_colaborador",
        "colaborador_aprovado",
        "colaborador_data_aprovacao",
        "colaborador_nota",
        "colaborador_motivo_reprovacao",
        "aprovacao_autor_identificador",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_treinamento_id",
        "migracao_turma_id",
        "migracao_turma_colaborador_id",
        "migracao_lcc_id",
        "migracao_operador_aprovacao_id",
    ]

    return out[ordered]


def map_operadores(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None
    out["operador_identificador"] = src["operador_identificador"] if "operador_identificador" in src.columns else None
    out["operador_cpf"] = src["operador_cpf"] if "operador_cpf" in src.columns else None
    out["operador_nome"] = src["operador_nome"] if "operador_nome" in src.columns else None
    out["operador_fone_fixo"] = src["operador_fone_fixo"] if "operador_fone_fixo" in src.columns else None
    out["operador_fone_celular"] = src["operador_fone_celular"] if "operador_fone_celular" in src.columns else None
    out["operador_observacoes"] = src["operador_observacoes"] if "operador_observacoes" in src.columns else None
    out["operador_email"] = src["operador_email"] if "operador_email" in src.columns else None
    out["operador_data_inclusao"] = src["operador_data_inclusao"] if "operador_data_inclusao" in src.columns else None
    out["operador_responsavel_nome"] = src["operador_responsavel_nome"] if "operador_responsavel_nome" in src.columns else None
    out["operador_responsavel_email"] = src["operador_responsavel_email"] if "operador_responsavel_email" in src.columns else None
    out["operador_responsavel_fone"] = src["operador_responsavel_fone"] if "operador_responsavel_fone" in src.columns else None
    out["operador_perfil"] = src["operador_perfil"] if "operador_perfil" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_operador_id"] = None

    out = coerce_datetime(out, [
        "operador_data_inclusao",
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "operador_identificador",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_operador_id",
    ])

    ordered = [
        "contratante_cnpj",
        "operador_identificador",
        "operador_cpf",
        "operador_nome",
        "operador_fone_fixo",
        "operador_fone_celular",
        "operador_observacoes",
        "operador_email",
        "operador_data_inclusao",
        "operador_responsavel_nome",
        "operador_responsavel_email",
        "operador_responsavel_fone",
        "operador_perfil",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_operador_id",
    ]

    return out[ordered]

def map_operadores_vinculos(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None
    out["contrato_numero"] = src["contrato_numero"] if "contrato_numero" in src.columns else None
    out["contratada_identificador"] = src["contratada_identificador"] if "contratada_identificador" in src.columns else None
    out["operador_identificador"] = src["operador_identificador"] if "operador_identificador" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_contrato_id"] = None
    out["migracao_contratada_id"] = None
    out["migracao_operador_id"] = None

    out = coerce_datetime(out, [
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "contratada_identificador",
        "operador_identificador",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_operador_id",
    ])

    ordered = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "operador_identificador",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_operador_id",
    ]

    return out[ordered]


def map_analises(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None
    out["contrato_numero"] = src["contrato_numero"] if "contrato_numero" in src.columns else None
    out["contratada_identificador"] = src["contratada_identificador"] if "contratada_identificador" in src.columns else None
    out["colaborador_identificador"] = src["colaborador_identificador"] if "colaborador_identificador" in src.columns else None
    out["competencia_data"] = src["competencia_data"] if "competencia_data" in src.columns else None
    out["competencia_tipo"] = src["competencia_tipo"] if "competencia_tipo" in src.columns else None

    out["analise_objeto_identificador"] = src["analise_objeto_identificador"] if "analise_objeto_identificador" in src.columns else None
    out["analise_objeto_tipo"] = src["analise_objeto_tipo"] if "analise_objeto_tipo" in src.columns else None
    out["analise_objeto_descricao"] = src["analise_objeto_descricao"] if "analise_objeto_descricao" in src.columns else None
    out["analise_objeto_conforme"] = src["analise_objeto_conforme"] if "analise_objeto_conforme" in src.columns else None
    out["analise_objeto_tipo_entrega"] = src["analise_objeto_tipo_entrega"] if "analise_objeto_tipo_entrega" in src.columns else None
    out["analise_data_analise"] = src["analise_data_analise"] if "analise_data_analise" in src.columns else None
    out["analise_autor_identificador"] = src["analise_autor_identificador"] if "analise_autor_identificador" in src.columns else None

    out["documento_identificador"] = src["documento_identificador"] if "documento_identificador" in src.columns else None
    out["documento_nome_arquivo"] = src["documento_nome_arquivo"] if "documento_nome_arquivo" in src.columns else None
    out["documento_caminho_storage"] = src["documento_caminho_storage"] if "documento_caminho_storage" in src.columns else None
    out["documento_tamanho_arquivo"] = src["documento_tamanho_arquivo"] if "documento_tamanho_arquivo" in src.columns else None
    out["documento_data_emissao"] = src["documento_data_emissao"] if "documento_data_emissao" in src.columns else None
    out["documento_data_validade"] = src["documento_data_validade"] if "documento_data_validade" in src.columns else None
    out["documento_data_postagem"] = src["documento_data_postagem"] if "documento_data_postagem" in src.columns else None
    out["documento_autor_identificador"] = src["documento_autor_identificador"] if "documento_autor_identificador" in src.columns else None

    out["justificativa_identificador"] = src["justificativa_identificador"] if "justificativa_identificador" in src.columns else None
    out["justificativa_texto"] = src["justificativa_texto"] if "justificativa_texto" in src.columns else None
    out["justificativa_data_postagem"] = src["justificativa_data_postagem"] if "justificativa_data_postagem" in src.columns else None
    out["justificativa_autor_identificador"] = src["justificativa_autor_identificador"] if "justificativa_autor_identificador" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_contrato_id"] = None
    out["migracao_contratada_id"] = None
    out["migracao_competencia_id"] = None
    out["migracao_analise_id"] = None
    out["migracao_colaborador_id"] = None
    out["migracao_lcc_id"] = None
    out["migracao_solicitacao_documentos_id"] = None
    out["migracao_objeto_analise_id"] = None
    out["migracao_procedimento_padrao_id"] = None
    out["migracao_procedimento_emissao_id"] = None
    out["migracao_procedimento_validade_id"] = None
    out["migracao_lsd_id"] = None
    out["migracao_documento_id"] = None
    out["migracao_justificativa_id"] = None

    out = coerce_date(out, [
        "competencia_data",
        "documento_data_emissao",
        "documento_data_validade",
    ])

    out = coerce_datetime(out, [
        "analise_data_analise",
        "documento_data_postagem",
        "justificativa_data_postagem",
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "contratada_identificador",
        "competencia_tipo",
        "analise_objeto_identificador",
        "documento_identificador",
        "documento_tamanho_arquivo",
        "justificativa_identificador",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_competencia_id",
        "migracao_analise_id",
        "migracao_colaborador_id",
        "migracao_lcc_id",
        "migracao_solicitacao_documentos_id",
        "migracao_objeto_analise_id",
        "migracao_procedimento_padrao_id",
        "migracao_procedimento_emissao_id",
        "migracao_procedimento_validade_id",
        "migracao_lsd_id",
        "migracao_documento_id",
        "migracao_justificativa_id",
    ])

    ordered = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "colaborador_identificador",
        "competencia_data",
        "competencia_tipo",
        "analise_objeto_identificador",
        "analise_objeto_tipo",
        "analise_objeto_descricao",
        "analise_objeto_conforme",
        "analise_objeto_tipo_entrega",
        "analise_data_analise",
        "analise_autor_identificador",
        "documento_identificador",
        "documento_nome_arquivo",
        "documento_caminho_storage",
        "documento_tamanho_arquivo",
        "documento_data_emissao",
        "documento_data_validade",
        "documento_data_postagem",
        "documento_autor_identificador",
        "justificativa_identificador",
        "justificativa_texto",
        "justificativa_data_postagem",
        "justificativa_autor_identificador",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_competencia_id",
        "migracao_analise_id",
        "migracao_colaborador_id",
        "migracao_lcc_id",
        "migracao_solicitacao_documentos_id",
        "migracao_objeto_analise_id",
        "migracao_procedimento_padrao_id",
        "migracao_procedimento_emissao_id",
        "migracao_procedimento_validade_id",
        "migracao_lsd_id",
        "migracao_documento_id",
        "migracao_justificativa_id",
    ]

    return out[ordered]
    
def map_procedimentos(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None
    out["contrato_numero"] = src["contrato_numero"] if "contrato_numero" in src.columns else None
    out["contratada_identificador"] = src["contratada_identificador"] if "contratada_identificador" in src.columns else None
    out["colaborador_identificador"] = src["colaborador_identificador"] if "colaborador_identificador" in src.columns else None
    out["competencia_data"] = src["competencia_data"] if "competencia_data" in src.columns else None
    out["competencia_tipo"] = src["competencia_tipo"] if "competencia_tipo" in src.columns else None

    out["analise_objeto_identificador"] = src["analise_objeto_identificador"] if "analise_objeto_identificador" in src.columns else None
    out["analise_objeto_tipo"] = src["analise_objeto_tipo"] if "analise_objeto_tipo" in src.columns else None
    out["analise_data_analise"] = src["analise_data_analise"] if "analise_data_analise" in src.columns else None
    out["analise_autor_identificador"] = src["analise_autor_identificador"] if "analise_autor_identificador" in src.columns else None

    out["procedimento_identificador"] = src["procedimento_identificador"] if "procedimento_identificador" in src.columns else None
    out["procedimento_conforme"] = src["procedimento_conforme"] if "procedimento_conforme" in src.columns else None
    out["procedimento_comentario"] = src["procedimento_comentario"].map(strip_html_text) if "procedimento_comentario" in src.columns else None
    out["procedimento_recomendacao"] = src["procedimento_recomendacao"].map(strip_html_text) if "procedimento_recomendacao" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_grupo_area_id"] = None
    out["migracao_contrato_id"] = None
    out["migracao_contratada_id"] = None
    out["migracao_competencia_id"] = None
    out["migracao_lcc_id"] = None
    out["migracao_analise_id"] = None
    out["migracao_colaborador_id"] = None
    out["migracao_objeto_analise_id"] = None
    out["migracao_procedimento_id"] = None
    out["migracao_operador_analise_id"] = None

    out = coerce_date(out, [
        "competencia_data",
    ])

    out = coerce_datetime(out, [
        "analise_data_analise",
    ])

    out = coerce_numeric(out, [
        "contratada_identificador",
        "competencia_tipo",
        "analise_objeto_identificador",
        "procedimento_identificador",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_grupo_area_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_competencia_id",
        "migracao_lcc_id",
        "migracao_analise_id",
        "migracao_colaborador_id",
        "migracao_objeto_analise_id",
        "migracao_procedimento_id",
        "migracao_operador_analise_id",
    ])

    ordered = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "colaborador_identificador",
        "competencia_data",
        "competencia_tipo",
        "analise_objeto_identificador",
        "analise_objeto_tipo",
        "analise_data_analise",
        "analise_autor_identificador",
        "procedimento_identificador",
        "procedimento_conforme",
        "procedimento_comentario",
        "procedimento_recomendacao",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_grupo_area_id",
        "migracao_contrato_id",
        "migracao_contratada_id",
        "migracao_competencia_id",
        "migracao_lcc_id",
        "migracao_analise_id",
        "migracao_colaborador_id",
        "migracao_objeto_analise_id",
        "migracao_procedimento_id",
        "migracao_operador_analise_id",
    ]

    return out[ordered]


def map_blacklist(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None
    out["colaborador_identificador"] = src["colaborador_identificador"] if "colaborador_identificador" in src.columns else None
    out["colaborador_cpf"] = src["colaborador_cpf"] if "colaborador_cpf" in src.columns else None
    out["colaborador_rg"] = src["colaborador_rg"] if "colaborador_rg" in src.columns else None
    out["colaborador_passaporte"] = src["colaborador_passaporte"] if "colaborador_passaporte" in src.columns else None
    out["colaborador_pis"] = src["colaborador_pis"] if "colaborador_pis" in src.columns else None
    out["colaborador_nome"] = src["colaborador_nome"] if "colaborador_nome" in src.columns else None
    out["colaborador_estrangeiro"] = src["colaborador_estrangeiro"] if "colaborador_estrangeiro" in src.columns else None
    out["colaborador_aposentado"] = src["colaborador_aposentado"] if "colaborador_aposentado" in src.columns else None
    out["colaborador_demitido"] = src["colaborador_demitido"] if "colaborador_demitido" in src.columns else None
    out["colaborador_data_demissao"] = src["colaborador_data_demissao"] if "colaborador_data_demissao" in src.columns else None
    out["colaborador_bloqueio_permanente"] = src["colaborador_bloqueio_permanente"] if "colaborador_bloqueio_permanente" in src.columns else None
    out["colaborador_motivo_bloqueio"] = src["colaborador_motivo_bloqueio"] if "colaborador_motivo_bloqueio" in src.columns else None
    out["blacklist_data_bloqueio"] = src["blacklist_data_bloqueio"] if "blacklist_data_bloqueio" in src.columns else None
    out["blacklist_autor_identificador"] = src["blacklist_autor_identificador"] if "blacklist_autor_identificador" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_blacklist_id"] = None

    out = coerce_date(out, [
        "colaborador_data_demissao",
    ])

    out = coerce_datetime(out, [
        "blacklist_data_bloqueio",
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "colaborador_identificador",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_blacklist_id",
    ])

    for col in [
        "colaborador_estrangeiro",
        "colaborador_aposentado",
        "colaborador_demitido",
        "colaborador_bloqueio_permanente",
    ]:
        if col in out.columns:
            out[col] = to_bit_series(out[col])

    ordered = [
        "contratante_cnpj",
        "colaborador_identificador",
        "colaborador_cpf",
        "colaborador_rg",
        "colaborador_passaporte",
        "colaborador_pis",
        "colaborador_nome",
        "colaborador_estrangeiro",
        "colaborador_aposentado",
        "colaborador_demitido",
        "colaborador_data_demissao",
        "colaborador_bloqueio_permanente",
        "colaborador_motivo_bloqueio",
        "blacklist_data_bloqueio",
        "blacklist_autor_identificador",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_blacklist_id",
    ]

    return out[ordered]


def map_riscos(df: pd.DataFrame, company_id: int) -> pd.DataFrame:
    src = df.copy()
    out = pd.DataFrame(index=src.index)

    out["contratante_cnpj"] = src["contratante_cnpj"] if "contratante_cnpj" in src.columns else None
    out["contrato_numero"] = src["contrato_numero"] if "contrato_numero" in src.columns else None
    out["contratada_identificador"] = src["contratada_identificador"] if "contratada_identificador" in src.columns else None
    out["colaborador_identificador"] = src["colaborador_identificador"] if "colaborador_identificador" in src.columns else None
    out["risco_atividade_identificador"] = src["risco_atividade_identificador"] if "risco_atividade_identificador" in src.columns else None
    out["risco_atividade_descricao"] = src["risco_atividade_descricao"] if "risco_atividade_descricao" in src.columns else None
    grupo_area = src["risco_atividade_grupo_descricao"] if "risco_atividade_grupo_descricao" in src.columns else pd.Series(pd.NA, index=src.index)
    if "grupo_area" in src.columns:
        grupo_area = grupo_area.combine_first(src["grupo_area"])
    out["risco_atividade_grupo_descricao"] = grupo_area.replace(r"^\s*$", pd.NA, regex=True).fillna("MF - MOM")
    out["analise_objeto_identificador"] = src["analise_objeto_identificador"] if "analise_objeto_identificador" in src.columns else None

    now = pd.Timestamp.now().round("ms")
    out["migracao_guid"] = [str(uuid.uuid4()) for _ in range(len(out))]
    out["migracao_status"] = 0
    out["migracao_data_status"] = now

    out["migracao_contratante_id"] = None
    out["migracao_risco_atividade_grupo_id"] = None
    out["migracao_risco_atividade_id"] = None
    out["migracao_lcc_id"] = None
    out["migracao_objeto_analise_id"] = None

    out = coerce_datetime(out, [
        "migracao_data_status",
    ])

    out = coerce_numeric(out, [
        "contratada_identificador",
        "colaborador_identificador",
        "risco_atividade_identificador",
        "analise_objeto_identificador",
        "migracao_status",
        "migracao_contratante_id",
        "migracao_risco_atividade_grupo_id",
        "migracao_risco_atividade_id",
        "migracao_lcc_id",
        "migracao_objeto_analise_id",
    ])

    ordered = [
        "contratante_cnpj",
        "contrato_numero",
        "contratada_identificador",
        "colaborador_identificador",
        "risco_atividade_identificador",
        "risco_atividade_descricao",
        "risco_atividade_grupo_descricao",
        "analise_objeto_identificador",
        "migracao_guid",
        "migracao_status",
        "migracao_data_status",
        "migracao_contratante_id",
        "migracao_risco_atividade_grupo_id",
        "migracao_risco_atividade_id",
        "migracao_lcc_id",
        "migracao_objeto_analise_id",
    ]

    return out[ordered]


# =========================
# EXECUÇÃO
# =========================

def process_parquet_streaming(
    engine: Engine,
    name: str,
    parquet_path: Path,
    company_id: int,
    chunksize: int,
    row_fallback: bool,
    error_dir: Path,
) -> ImportStats:
    table_name = TARGET_TABLES[name]
    total = ImportStats(table_name=table_name, parquet_name=parquet_path.name)
    mapper = map_analises if name == "analises" else map_procedimentos

    print(f"[INFO] Lendo {name} em lotes de {chunksize} linhas: {parquet_path}")
    table_meta = get_table_columns(engine, table_name)
    diagnostics_printed = False

    for batch_index, df_batch in enumerate(iter_parquet_batches(parquet_path, chunksize), start=1):
        batch_rows = len(df_batch)
        total.read_rows += batch_rows

        if df_batch.empty:
            continue

        print(f"[INFO] {name}: preparando lote {batch_index} com {batch_rows} linhas...")
        df_mapped = mapper(df_batch, company_id)
        total.prepared_rows += len(df_mapped)

        batch_stats = insert_dataframe(
            engine=engine,
            table_name=table_name,
            parquet_name=f"{parquet_path.name}#lote-{batch_index}",
            df=df_mapped,
            chunksize=chunksize,
            row_fallback=row_fallback,
            error_dir=error_dir,
            table_meta=table_meta,
            show_diagnostics=not diagnostics_printed,
        )
        diagnostics_printed = True
        total.inserted_rows += batch_stats.inserted_rows
        total.failed_rows += batch_stats.failed_rows

    if total.read_rows == 0:
        print(f"[INFO] {table_name}: parquet vazio, nada a inserir.")

    return total


def process_analises_streaming(
    engine: Engine,
    parquet_path: Path,
    company_id: int,
    chunksize: int,
    row_fallback: bool,
    error_dir: Path,
) -> ImportStats:
    return process_parquet_streaming(
        engine=engine,
        name="analises",
        parquet_path=parquet_path,
        company_id=company_id,
        chunksize=chunksize,
        row_fallback=row_fallback,
        error_dir=error_dir,
    )


def process_procedimentos_streaming(
    engine: Engine,
    parquet_path: Path,
    company_id: int,
    chunksize: int,
    row_fallback: bool,
    error_dir: Path,
) -> ImportStats:
    return process_parquet_streaming(
        engine=engine,
        name="procedimentos",
        parquet_path=parquet_path,
        company_id=company_id,
        chunksize=chunksize,
        row_fallback=row_fallback,
        error_dir=error_dir,
    )

def process_one(
    engine: Engine,
    name: str,
    parquet_path: Path,
    company_id: int,
    chunksize: int,
    row_fallback: bool,
    error_dir: Path,
) -> ImportStats:
    if name == "analises":
        return process_analises_streaming(
            engine=engine,
            parquet_path=parquet_path,
            company_id=company_id,
            chunksize=chunksize,
            row_fallback=row_fallback,
            error_dir=error_dir,
        )
    if name == "procedimentos":
        return process_procedimentos_streaming(
            engine=engine,
            parquet_path=parquet_path,
            company_id=company_id,
            chunksize=chunksize,
            row_fallback=row_fallback,
            error_dir=error_dir,
        )

    print(f"[INFO] Lendo {name}...")
    df = pd.read_parquet(parquet_path)
    original_rows = len(df)

    if name == "contratos":
        df = map_contratos(df, company_id)
        df = filter_existing_contratos(engine, TARGET_TABLES[name], df)
    elif name == "colaboradores":
        df = map_colaboradores(df, company_id)
        df = filter_existing_colaboradores(engine, TARGET_TABLES[name], df)
    elif name == "integracoes":
        df = map_integracoes(df, company_id)
    elif name == "operadores":
        df = map_operadores(df, company_id)
        df = filter_existing_operadores(engine, TARGET_TABLES[name], df)
    elif name == "operadores_vinculos":
        df = map_operadores_vinculos(df, company_id)
        df = filter_existing_operadores_vinculos(engine, TARGET_TABLES[name], df)
    elif name == "blacklist":
        df = map_blacklist(df, company_id)
    elif name == "riscos":
        df = map_riscos(df, company_id)
    elif name == "procedimentos":
        df = map_procedimentos(df, company_id)
    else:
        raise ValueError(name)

    stats = insert_dataframe(
        engine=engine,
        table_name=TARGET_TABLES[name],
        parquet_name=parquet_path.name,
        df=df,
        chunksize=chunksize,
        row_fallback=row_fallback,
        error_dir=error_dir,
    )
    stats.read_rows = original_rows
    stats.prepared_rows = len(df)
    return stats


def print_summary(stats_list: list[ImportStats]) -> None:
    print("\n========== RESUMO ==========")
    for s in stats_list:
        print(
            f"{s.parquet_name} -> {s.table_name} | "
            f"lidas={s.read_rows} | preparadas={s.prepared_rows} | "
            f"inseridas={s.inserted_rows} | falhas={s.failed_rows}"
        )


def main() -> int:
    args = parse_args()
    engine = build_engine(args)
    error_dir = Path(args.error_dir)

    with engine.connect() as conn:
        db_name = conn.execute(text("SELECT DB_NAME()")).scalar()
        print(f"[OK] Conectado no banco: {db_name}")

    parquet_files = {
        "contratos": Path(args.contratos),
        "colaboradores": Path(args.colaboradores),
        "integracoes": Path(args.integracoes),
        "operadores": Path(args.operadores),
        "operadores_vinculos": Path(args.operadores_vinculos),
        "analises": Path(args.analises),
        "blacklist": Path(args.blacklist),
        "riscos": Path(args.riscos),
        "procedimentos": Path(args.procedimentos),
    }

    names = [args.only] if args.only else [
        "operadores",
        "contratos",
        "colaboradores",
        "analises",
        "procedimentos",
        "riscos",
        "blacklist",
        "integracoes",
        "operadores_vinculos",
    ]

    for name in names:
        path = parquet_files[name]
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    contratante_cnpj = normalize_cli_text(args.contratante_cnpj)
    should_cleanup = args.truncate or contratante_cnpj is not None
    preserve_status_3_groups = {"contratos", "colaboradores", "operadores", "operadores_vinculos"}

    if should_cleanup:
        if contratante_cnpj is not None:
            print(f"[INFO] Limpando tabelas para contratante_cnpj={contratante_cnpj!r}...")
        else:
            print("[INFO] Limpando tabelas...")
        for name in reversed(names):
            if contratante_cnpj is not None:
                delete_by_contratante_cnpj(
                    engine,
                    TARGET_TABLES[name],
                    contratante_cnpj,
                    preserve_status_3=name in preserve_status_3_groups,
                )
            elif name in preserve_status_3_groups:
                delete_preserving_status_3(engine, TARGET_TABLES[name])
            else:
                truncate_table(engine, TARGET_TABLES[name])

    stats_list: list[ImportStats] = []
    for name in names:
        stats = process_one(
            engine=engine,
            name=name,
            parquet_path=parquet_files[name],
            company_id=args.company_id,
            chunksize=args.chunksize,
            row_fallback=args.row_fallback,
            error_dir=error_dir,
        )
        stats_list.append(stats)
        if name == "colaboradores":
            delete_colaboradores_duplicados_motivo_demissao_nulo(engine, TARGET_TABLES["colaboradores"])
        if name == "contratos":
            delete_contratos_status_diferente_3_duplicados_com_status3(engine, TARGET_TABLES["contratos"])

    print_summary(stats_list)
    print("[OK] Importação finalizada.")
    return 0


if __name__ == "__main__":
    from migracao_logging import run_script

    raise SystemExit(run_script(main, __file__))
