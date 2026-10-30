#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from migracao_contrato_mapping import apply_contrato_mapping
from migracao_export_dates import adjust_export_datetimes
from migracao_project_filter import CONTRATADAS_CTE, parse_project_args
import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("export_riscos")


PARQUET_COLUMN_ORDER = [
    "contratante_cnpj",
    "contrato_numero",
    "contratada_identificador",
    "colaborador_identificador",
    "risco_atividade_identificador",
    "risco_atividade_descricao",
    "risco_atividade_grupo_descricao",
    "analise_objeto_identificador",
]

INTEGER_COLS = [
    "contratada_identificador",
    "colaborador_identificador",
    "risco_atividade_identificador",
    "analise_objeto_identificador",
]

STRING_COLS = [col for col in PARQUET_COLUMN_ORDER if col not in INTEGER_COLS]


SQL_SELECT = "with " + CONTRATADAS_CTE + r"""
, parametros as (
    select
        %(company_id)s::integer as company_id,
        %(project_ids)s::integer[] as project_ids
),

projetos_base as (
    select
        PRJ.id,
        PRJ.name as contrato_numero,
        CTR.cnpj as contratante_cnpj
    from parametros as PAR
         inner join project as PRJ
                 on PRJ.company_id = PAR.company_id
                and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
                and PRJ.deleted = false
                and (
                        PAR.project_ids is null
                     or PRJ.id = any(PAR.project_ids)
                )
         inner join company as CTR
                 on CTR.id = PRJ.company_id
                and CTR.deleted = false
),

contratos_colaborador as (
    select distinct
        CTT.project_id,
        CTT.employee_id
    from contract as CTT
         inner join projetos_base as PB
                 on PB.id = CTT.project_id
    where CTT.deleted = false
),

pendencias_base as (
    select
        PCY.id as pendencia_id,
        PCY.project_id,
        PCY.employee_id,
        PCY.pendency_id
    from projetos_base as PB
         inner join pendency as PCY
                 on PCY.project_id = PB.id
                and (%(pendency_ids)s::integer[] is null or PCY.pendency_id = any(%(pendency_ids)s::integer[]))
                and (
                        PCY.deleted = false
                     or (
                            PCY.deleted = true
                        and (
                            %(considerar_inactive)s and (
                                PCY.plan_id = 1 and PCY.active_status in (
                                    'INACTIVE_BY_MOVEMENT', 'INACTIVE_BY_PROJECT_DEACTIVATION',
                                    'INACTIVE_BY_PROJECT_COMPANY_INACTIVATION'
                                )
                            )
                        )
                     )
                )
),

pendencias_colaboradores as (
    select distinct
        PB.project_id,
        PB.pendency_id,
        PB.employee_id
    from pendencias_base as PB
    where PB.employee_id is not null

    union

    select distinct
        PB.project_id,
        PB.pendency_id,
        PCY_EMP.employee_id
    from pendencias_base as PB
         inner join pendency_employee as PCY_EMP
                 on PCY_EMP.pendency_id = PB.pendencia_id
                and PCY_EMP.deleted = false
    where PCY_EMP.employee_id is not null
),

riscos_base as (
    select distinct
        RSK_SCH.project_id,
        RSK_SCH_EMP.employee_id,
        RSK_ACT.id as risco_atividade_identificador,
        RSK_ACT.name as risco_atividade_descricao,
        coalesce(FAC.name, 'MF - MOM') as risco_atividade_grupo_descricao,
        RSK_ACT_PCY.system_pendency_id as analise_objeto_identificador
    from projetos_base as PB
         inner join pendencias_colaboradores as PC
                 on PC.project_id = PB.id
         inner join risk_schedule_employee as RSK_SCH_EMP
                 on RSK_SCH_EMP.employee_id = PC.employee_id
                and RSK_SCH_EMP.deleted = false
         inner join risk_schedule as RSK_SCH
                 on RSK_SCH.id = RSK_SCH_EMP.risk_schedule_id
                and RSK_SCH.project_id = PB.id
                and RSK_SCH.deleted = false
         inner join risk_schedule_activity as RSK_SCH_ACT
                 on RSK_SCH_ACT.risk_schedule_id = RSK_SCH.id
                and RSK_SCH_ACT.deleted = false
         inner join risk_activity as RSK_ACT
                 on RSK_ACT.id = RSK_SCH_ACT.risk_activity_id
                and RSK_ACT.deleted = false
         inner join risk_activity_system_pendency as RSK_ACT_PCY
                 on RSK_ACT_PCY.risk_activity_id = RSK_ACT.id
                and RSK_ACT_PCY.system_pendency_id = PC.pendency_id
                and RSK_ACT_PCY.deleted = false
         left join project_location as PRJ_LOC
                on PRJ_LOC.id = RSK_SCH.project_location_id
               and PRJ_LOC.deleted = false
         left join facility as FAC
                on FAC.id = PRJ_LOC.facility_id
               and FAC.deleted = false
)

select
       PRJ.contratante_cnpj,
       PRJ.contrato_numero,
       PRJ.id as _contrato_project_id,
       CPN.id as contratada_identificador,
       EMP.id as colaborador_identificador,
       RB.risco_atividade_identificador,
       RB.risco_atividade_descricao,
       RB.risco_atividade_grupo_descricao,
       RB.analise_objeto_identificador
from pendencias_colaboradores as PC
     inner join contratos_colaborador as CTT
             on CTT.project_id = PC.project_id
            and CTT.employee_id = PC.employee_id
     inner join riscos_base as RB
             on RB.project_id = PC.project_id
            and RB.employee_id = PC.employee_id
            and RB.analise_objeto_identificador = PC.pendency_id
     inner join projetos_base as PRJ
             on PRJ.id = PC.project_id
     inner join employee as EMP
             on EMP.id = PC.employee_id
            and EMP.deleted = false
     inner join company as CPN
             on CPN.id = EMP.company_id
             and (%(contratada_cnpjs)s::text[] is null or exists (
                 select 1 from contratadas_filtradas as CF
                 where CF.project_id = PRJ.id and CF.company_id = CPN.id
             ))
            and CPN.deleted = false
"""


PROJECT_IDS_SQL = "with " + CONTRATADAS_CTE + r"""
select PRJ.id
from project as PRJ
where PRJ.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF where CF.project_id = PRJ.id
  ))
  and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
  and PRJ.deleted = false
  and exists (
      select 1
      from pendency as PCY
      where (%(pendency_ids)s::integer[] is null or PCY.pendency_id = any(%(pendency_ids)s::integer[]))
        and PCY.project_id = PRJ.id
        and (
                PCY.deleted = false
             or (
                    PCY.deleted = true
                and (
                    %(considerar_inactive)s and (
                        PCY.plan_id = 1 and PCY.active_status in (
                            'INACTIVE_BY_MOVEMENT', 'INACTIVE_BY_PROJECT_DEACTIVATION',
                            'INACTIVE_BY_PROJECT_COMPANY_INACTIVATION'
                        )
                    )
                )
             )
        )
  )
  and exists (
      select 1
      from risk_schedule as RSK_SCH
           inner join risk_schedule_activity as RSK_SCH_ACT
                   on RSK_SCH_ACT.risk_schedule_id = RSK_SCH.id
                  and RSK_SCH_ACT.deleted = false
      where RSK_SCH.project_id = PRJ.id
        and RSK_SCH.deleted = false
  )
order by PRJ.id
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta riscos para Parquet.")
    p.add_argument("--company-id", type=int, required=True)

    p.add_argument("--out", required=True, help="Arquivo parquet de saída")
    p.add_argument("--grupos-csv", required=True, help="CSV com de/para de grupo de área")
    p.add_argument("--grupos-encoding", default="utf-8")
    p.add_argument("--grupos-sep", default=";")

    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--db", dest="dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", default=None)
    p.add_argument("--sslmode", default="require")

    p.add_argument("--chunksize", type=int, default=50000, help="Linhas por chunk")
    p.add_argument(
        "--project-block-size",
        type=int,
        default=int(os.getenv("MIGRACAO_RISCOS_PROJECT_BLOCK_SIZE", "50")),
        help="Quantidade de projetos por bloco. Use 0 para executar a empresa inteira em uma unica query.",
    )
    p.add_argument("--grupos-log-limit", type=int, default=20, help="Quantidade máxima de exemplos sem correspondência por chunk")

    return parse_project_args(p)


def ensure_output_path(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _normalize_key_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def load_grupos_csv(csv_path: str, sep: str = ";", encoding: str = "latin1") -> pd.DataFrame:
    df_csv = pd.read_csv(
        csv_path,
        sep=sep,
        header=None,
        encoding=encoding,
        dtype="string",
    )

    if df_csv.shape[1] < 2:
        raise ValueError("O CSV de grupos deve possuir ao menos 2 colunas.")

    df_csv = df_csv.iloc[:, :2].copy()
    df_csv.columns = [
        "contrato_numero_csv",
        "novo_risco_atividade_grupo_descricao",
    ]

    for col in df_csv.columns:
        df_csv[col] = _normalize_key_series(df_csv[col])

    df_csv = df_csv.drop_duplicates(
        subset=["contrato_numero_csv"],
        keep="first",
    ).reset_index(drop=True)

    return df_csv


def apply_grupos_mapping(
    chunk: pd.DataFrame,
    grupos_df: pd.DataFrame,
    log_limit: int,
) -> tuple[pd.DataFrame, int, int, int, list[str]]:
    """
    Para cada linha do SQL:
    - procura correspondência no CSV apenas pela coluna contrato_numero
    - se encontrar, substitui risco_atividade_grupo_descricao pela coluna 2 do CSV
    - registra exemplos quando não encontrar no CSV
    - depois remove linhas duplicadas geradas
    """
    out = chunk.copy()

    out["contrato_numero"] = _normalize_key_series(out["contrato_numero"])
    out["risco_atividade_grupo_descricao"] = _normalize_key_series(out["risco_atividade_grupo_descricao"])

    merged = out.merge(
        grupos_df,
        how="left",
        left_on=["contrato_numero"],
        right_on=["contrato_numero_csv"],
    )

    matched_mask = merged["novo_risco_atividade_grupo_descricao"].notna()
    matched = int(matched_mask.sum())
    not_found = int((~matched_mask).sum())

    not_found_examples = (
        merged.loc[~matched_mask, ["contrato_numero", "risco_atividade_grupo_descricao"]]
        .drop_duplicates()
        .head(log_limit)
    )

    example_messages = [
        f"SEM MATCH | contrato_numero={row['contrato_numero']} | grupo_atual={row['risco_atividade_grupo_descricao']}"
        for _, row in not_found_examples.iterrows()
    ]

    merged["risco_atividade_grupo_descricao"] = merged["novo_risco_atividade_grupo_descricao"].fillna(
        merged["risco_atividade_grupo_descricao"]
    )

    merged = merged.drop(
        columns=[
            "contrato_numero_csv",
            "novo_risco_atividade_grupo_descricao",
        ],
        errors="ignore",
    )

    before_dedup = len(merged)
    merged = merged.drop_duplicates().reset_index(drop=True)
    duplicates_removed = before_dedup - len(merged)

    return merged, matched, not_found, duplicates_removed, example_messages



def build_parquet_schema() -> pa.Schema:
    return pa.schema([
        ("contratante_cnpj", pa.string()),
        ("contrato_numero", pa.string()),
        ("contratada_identificador", pa.int64()),
        ("colaborador_identificador", pa.int64()),
        ("risco_atividade_identificador", pa.int64()),
        ("risco_atividade_descricao", pa.string()),
        ("risco_atividade_grupo_descricao", pa.string()),
        ("analise_objeto_identificador", pa.int64()),
    ])


def normalize_batch(df: pd.DataFrame) -> pd.DataFrame:
    missing_cols = [col for col in PARQUET_COLUMN_ORDER if col not in df.columns]
    if missing_cols:
        raise RuntimeError(f"Query nao retornou colunas esperadas: {missing_cols}")

    columns = PARQUET_COLUMN_ORDER + [col for col in ["_contrato_project_id"] if col in df.columns]
    df = df[columns].copy()

    for col in INTEGER_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in STRING_COLS:
        df[col] = df[col].astype("string")

    return df


def empty_table(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pydict(
        {field.name: pa.array([], type=field.type) for field in schema},
        schema=schema,
    )


def chunked(values: list[int], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def build_project_blocks(conn, args: argparse.Namespace) -> list[list[int] | None]:
    if args.project_block_size <= 0:
        return [None]

    with conn.cursor() as cur:
        cur.execute(PROJECT_IDS_SQL, {"company_id": args.company_id, "project_names": args.project_names, "pendency_ids": args.pendency_ids, "contratada_cnpjs": args.contratada_cnpjs, "considerar_inactive": args.considerar_inactive})
        project_ids = [row[0] for row in cur.fetchall()]

    if not project_ids:
        return []

    return list(chunked(project_ids, args.project_block_size))


def export_to_parquet(args: argparse.Namespace, out_path: Path):
    if args.chunksize <= 0:
        raise ValueError("--chunksize precisa ser maior que zero.")
    if args.project_block_size < 0:
        raise ValueError("--project-block-size precisa ser maior ou igual a zero.")

    grupos_df = load_grupos_csv(
        csv_path=args.grupos_csv,
        sep=args.grupos_sep,
        encoding=args.grupos_encoding,
    )
    logger.info("CSV grupos carregado: %s registros de de/para", len(grupos_df))

    schema = build_parquet_schema()
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")
    sql_select = SQL_SELECT.strip().rstrip(";")

    if tmp_path.exists():
        tmp_path.unlink()

    conn = None
    parquet_writer = None
    total = 0
    total_matched = 0
    total_duplicates_removed = 0

    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            sslmode=args.sslmode,
        )
        conn.set_session(readonly=True)
        project_blocks = build_project_blocks(conn, args)

        if args.project_block_size > 0:
            logger.info(
                "Executando consulta para company_id=%s em %s bloco(s) de ate %s projeto(s), com chunks de %s linhas...",
                args.company_id,
                len(project_blocks),
                args.project_block_size,
                args.chunksize,
            )
        else:
            logger.info(
                "Executando consulta para company_id=%s em lote unico, com chunks de %s linhas...",
                args.company_id,
                args.chunksize,
            )

        parquet_writer = pq.ParquetWriter(tmp_path, schema, compression="snappy")
        for block_index, project_ids in enumerate(project_blocks, start=1):
            cursor_name = f"export_riscos_{os.getpid()}_{block_index}"
            params = {"company_id": args.company_id, "project_ids": project_ids, "project_names": args.project_names, "pendency_ids": args.pendency_ids, "contratada_cnpjs": args.contratada_cnpjs, "considerar_inactive": args.considerar_inactive}
            if project_ids is None:
                logger.info("Bloco %s/%s: empresa inteira.", block_index, len(project_blocks))
            else:
                logger.info(
                    "Bloco %s/%s: %s projeto(s), project_id %s a %s.",
                    block_index,
                    len(project_blocks),
                    len(project_ids),
                    project_ids[0],
                    project_ids[-1],
                )

            block_total = 0
            with conn.cursor(name=cursor_name) as cur:
                cur.itersize = args.chunksize
                cur.execute(sql_select, params)

                while True:
                    rows = cur.fetchmany(args.chunksize)
                    if not rows:
                        break

                    sql_rows = len(rows)
                    cols = [desc[0] for desc in cur.description]
                    chunk = pd.DataFrame.from_records(rows, columns=cols)
                    chunk = normalize_batch(chunk)
                    chunk, matched, not_found, duplicates_removed, example_messages = apply_grupos_mapping(
                        chunk, grupos_df, args.grupos_log_limit
                    )
                    chunk = normalize_batch(chunk)

                    logger.info("Linhas do SQL processadas neste chunk: %s", sql_rows)
                    logger.info("Registros com correspond?ncia no CSV neste chunk: %s", matched)
                    logger.info("Registros sem correspond?ncia no CSV neste chunk: %s", not_found)
                    for message in example_messages:
                        logger.info(message)
                    logger.info("Linhas duplicadas removidas antes do parquet neste chunk: %s", duplicates_removed)

                    chunk = apply_contrato_mapping(chunk, args)
                    table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
                    table = adjust_export_datetimes(table)
                    parquet_writer.write_table(table)

                    block_total += len(chunk)
                    total += len(chunk)
                    total_matched += matched
                    total_duplicates_removed += duplicates_removed
                    logger.info("Registros exportados acumulados: %s", total)

            logger.info("Bloco %s/%s concluido: %s registros.", block_index, len(project_blocks), block_total)

        if not project_blocks:
            logger.info("Nenhum projeto com pendencias encontrado para company_id=%s.", args.company_id)

        if parquet_writer is not None and total == 0:
            parquet_writer.write_table(empty_table(schema))

    except Exception:
        if parquet_writer is not None:
            parquet_writer.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    else:
        if parquet_writer is not None:
            parquet_writer.close()
        tmp_path.replace(out_path)
    finally:
        if conn is not None:
            conn.close()

    logger.info("Arquivo parquet gerado com sucesso: %s", out_path)
    logger.info("Total de registros exportados: %s", total)
    logger.info("Total de correspond?ncias no CSV: %s", total_matched)
    logger.info("Total de linhas duplicadas removidas: %s", total_duplicates_removed)

def main():
    args = parse_args()
    out_path = ensure_output_path(args.out)
    export_to_parquet(args, out_path)


if __name__ == "__main__":
    from migracao_logging import run_script

    raise SystemExit(run_script(main, __file__))
