#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from migracao_contrato_mapping import apply_contrato_mapping
from migracao_sql_export import iter_sql_chunks
from migracao_project_filter import CONTRATADAS_CTE, parse_project_args
import logging
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

# Tenta deixar o console em UTF-8 quando possível
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("export_contratos")

SQL_SELECT = "with " + CONTRATADAS_CTE + r"""
select distinct
        CPN.cnpj            as contratante_cnpj
      , FAC.name            as contrato_grupo_area
      , DPT.name            as contrato_area
      , NULL                as contrato_familia
      , NULL                as contrato_subfamilia
      , NULL                as contrato_unidades
      , PRJ.name as contrato_numero
      , PRJ.id as _contrato_project_id
      , case when coalesce((select 1
         from project_company_plan as PCP
         where PCP.project_company_id = PRJ_CPN.id
         and   PCP.plan_id = 2
         and   PCP.deleted = false 
         limit 1), 0) = 1 and to_char(PRJ.start_date, 'DD') <> '01' then date_trunc('month', PRJ.start_date)::timestamp
         else PRJ.start_date
        end		            as contrato_data_inicio_vigencia
      , PRJ.end_date        as contrato_data_fim_vigencia
      , case
          when (
              PRJ_CPN.id = (
                  select C.id
                  from project_company C
                  where C.project_id = PRJ.id
                    and C.deleted = false
                    and C.relation_type_id = 2
                  order by C.created_at
                  limit 1
              )
              or (
                  PRJ_CPN.id = (
                      select C.id
                      from project_company C
                      where C.project_id = PRJ.id
                        and C.deleted = false
                        and C.relation_type_id = 3
                        and not exists (
                            select 1
                            from project_company C1
                            where C1.project_id = PRJ.id
                              and C1.deleted = false
                              and C1.relation_type_id = 2
                        )
                      order by C.created_at
                      limit 1
                  )
              )
          )
          then case
                 when exists (
                     select 1
                     from project_company PC
                     inner join project_company_plan PCP
                             on PCP.project_company_id = PC.id
                            and PCP.plan_id = 1
                            and PCP.deleted = false
                     where PC.project_id = PRJ.id
                       and PC.deleted = false
                 )
                 then 1
                 else 0
               end
          else case
                 when exists (
                     select 1
                     from project_company_plan PCP
                     where PCP.project_company_id = PRJ_CPN.id
                       and PCP.plan_id = 1
                       and PCP.deleted = false
                 )
                 then 1
                 else 0
               end
        end                 as contrato_servico_mobilizacao
      , case
          when (
              PRJ_CPN.id = (
                  select C.id
                  from project_company C
                  where C.project_id = PRJ.id
                    and C.deleted = false
                    and C.relation_type_id = 2
                  order by C.created_at
                  limit 1
              )
              or (
                  PRJ_CPN.id = (
                      select C.id
                      from project_company C
                      where C.project_id = PRJ.id
                        and C.deleted = false
                        and C.relation_type_id = 3
                        and not exists (
                            select 1
                            from project_company C1
                            where C1.project_id = PRJ.id
                              and C1.deleted = false
                              and C1.relation_type_id = 2
                        )
                      order by C.created_at
                      limit 1
                  )
              )
          )
          then case
                 when exists (
                     select 1
                     from project_company PC
                     inner join project_company_plan PCP
                             on PCP.project_company_id = PC.id
                            and PCP.plan_id = 2
                            and PCP.deleted = false
                     where PC.project_id = PRJ.id
                       and PC.deleted = false
                 )
                 then 1
                 else 0
               end
          else case
                 when exists (
                     select 1
                     from project_company_plan PCP
                     where PCP.project_company_id = PRJ_CPN.id
                       and PCP.plan_id = 2
                       and PCP.deleted = false
                 )
                 then 1
                 else 0
               end
        end                 as contrato_servico_trabalhista
      , 0                   as contrato_servico_veiculo
      , 0                   as contrato_servico_ambiental
      , PRJ.contract as contrato_numero_ordem_servico
      , NULL                as contrato_numero_contrato_juridico
      , 'A definir  - Migração' as contrato_local_servico
      , NULL                as contrato_cei
      , NULL                as contrato_valor_servico
      , NULL                as contrato_valor_equipamento
      , NULL                as contrato_valor_material
      , NULL                as contrato_valor_reembolso
      , 1                   as contrato_utiliza_mao_obra
      , 0                   as contrato_classificacao_objeto_contratual
      , NULL                as contrato_objeto_contratual
      , case when coalesce((select 1
         from project_company_plan as PCP
         where PCP.project_company_id = PRJ_CPN.id
         and   PCP.plan_id = 2
         and   PCP.deleted = false 
         limit 1), 0) = 1 and to_char(PRJ.start_date, 'DD') <> '01' then 'Data de início modificada na migração de ' || to_char(PRJ.start_date, 'DD/MM/YYYY') || ' para ' || to_char(PRJ.start_date, '01/MM/YYYY')
         else NULL
        end		                            as contrato_observacoes_gerais
      , PRJ.description     as contrato_descricao
      , NULL                as contrato_custeio_alimentacao
      , NULL                as contrato_custeio_abastecimento
      , (select ACC.id
         from   account_company as ACC_CPN
                inner join account as ACC
                        on ACC.id = ACC_CPN.account_id
         where  ACC_CPN.company_id = PRJ.company_id
           and  ACC_CPN.deleted = false
           and  ACC.active = true
           and  ACC.deleted = false
         order by ACC_CPN.account_id
         limit 1)           as contrato_responsavel_identificador
      , PRV.id              as contratada_identificador
      , PRV.cnpj            as contratada_cnpj
      , PRV.name            as contratada_razao_social
      , PRV.trading_name    as contratada_fantasia
      , 'PJ'                as contratada_tipo_pessoa
      , case
          when (PRJ_CPN.id = (select C.id
                              from   project_company as C
                              where  C.project_id = PRJ.id
                              and    C.deleted = false
                              and    C.relation_type_id = 2
                              order by C.created_at
                              limit 1))
               or (PRJ_CPN.id = (select C.id
                              from   project_company as C
                              where  C.project_id = PRJ.id
                              and    C.deleted = false
                              and    C.relation_type_id = 3
                              and    not exists (select C1.id
                                                 from   project_company as C1
                                                 where  C1.project_id = PRJ.id
                                                 and    C1.deleted = false
                                                 and    C1.relation_type_id = 2)
                              order by C.created_at
                              limit 1))                             then 'Contratada'
          else 'Subcontratada'
          end               as contratada_tipo
      , case when coalesce((select 1
         from project_company_plan as PCP
         where PCP.project_company_id = PRJ_CPN.id
         and   PCP.plan_id = 2
         and   PCP.deleted = false 
         limit 1), 0) = 1 and to_char(PRJ.start_date, 'DD') <> '01' then date_trunc('month', PRJ.start_date)::timestamp
         else PRJ.start_date
        end		            as contratada_data_inicio_trabalhista
      , PRJ.end_date        as contratada_data_fim_trabalhista
      , PRJ.start_date      as contratada_data_inicio_mobilizacao
      , PRJ.end_date        as contratada_data_fim_mobilizacao
      , NULL                as contratada_inscricao_estadual
      , NULL                as contratada_inscricao_municipal
      , 'Não Informado - Migração Bex > V3' as contratada_endereco
      , 0                   as contratada_numero
      , NULL                as contratada_complemento
      , 'Não Informado - Migração Bex > V3' as contratada_bairro
      , 'Não Informado - Migração Bex > V3' as contratada_cidade
      , 'PE'                as contratada_uf
      , '00000-000'         as contratada_cep
      , NULL                as contratada_fone
      , NULL                as contratada_fax
      , coalesce((select coalesce(ACC.id::text, 'migracao@bernhoeft.com.br')
                  from   project_company_account as PRJ_ACC
                         inner join account as ACC
                                 on ACC.id = PRJ_ACC.account_id
                  where  PRJ_ACC.project_company_id = PRJ_CPN.id
                    and  PRJ_ACC.deleted = false
                    and  ACC.active = true
                    and  ACC.deleted = false
                  order by PRJ_ACC.account_id
                  limit 1), 'migracao@bernhoeft.com.br') as contratada_responsavel_identificador
from project as PRJ
     inner join project_company as PRJ_CPN
                 on PRJ_CPN.project_id = PRJ.id
                and PRJ_CPN.deleted = false
     inner join company PRV
                 on PRV.id = PRJ_CPN.company_id
                and PRV.deleted = false
     left  join project_location as PRJ_LOC
                 on PRJ_LOC.project_id = PRJ.id
                and PRJ_LOC.deleted = false
     left  join facility as FAC
                 on FAC.id = PRJ_LOC.facility_id
                and FAC.deleted = false
     left  join project_location_department as PLD
                 on PLD.project_location_id = PRJ_LOC.id
                and PLD.deleted = false
     left  join department as DPT
                 on DPT.id = PLD.department_id
                and DPT.deleted = false
     inner join company as CPN
                 on CPN.id = PRJ.company_id
where PRJ.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF
      where CF.project_id = PRJ.id and CF.company_id = PRV.id
  ))
  and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
  and PRJ.deleted = false
order by contrato_numero, contrato_data_inicio_vigencia
"""

MOJIBAKE_MARKERS = ("Ã", "Â", "â", "ð", "\ufffd")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta contratos para Parquet.")
    p.add_argument("--company-id", type=int, required=True)

    p.add_argument("--out", required=True, help="Arquivo parquet de saída")
    p.add_argument("--grupoareas-csv", required=True, help="Planilha/CSV com 3 colunas: contrato_numero, novo_grupo_area, nova_area")
    p.add_argument("--grupoareas-encoding", default="latin1", help="Encoding da planilha/CSV de grupo/área (default: latin1)")
    p.add_argument("--grupoareas-sep", default=";", help="Separador da planilha/CSV de grupo/área (default: ;)")
    p.add_argument("--grupoareas-log-limit", type=int, default=20, help="Quantidade máxima de exemplos detalhados por chunk")

    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--db", dest="dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", default=None)
    p.add_argument("--sslmode", default="require")

    p.add_argument("--chunksize", type=int, default=50000, help="Linhas por chunk")
    return parse_project_args(p)


def ensure_output_path(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _safe_str(value):
    if pd.isna(value):
        return None
    return str(value)


def fix_mojibake_text(value):
    if value is None or pd.isna(value):
        return value

    text = str(value)

    # normaliza espaços e forma unicode
    def _normalize(t: str) -> str:
        return unicodedata.normalize("NFC", t)

    text = _normalize(text)

    if any(marker in text for marker in MOJIBAKE_MARKERS):
        # tentativa comum: texto UTF-8 interpretado como latin1/cp1252
        for source_enc in ("latin1", "cp1252"):
            try:
                repaired = text.encode(source_enc, errors="strict").decode("utf-8", errors="strict")
                repaired = _normalize(repaired)
                # aceita o reparo se remove marcadores típicos de mojibake
                if repaired != text and sum(m in repaired for m in MOJIBAKE_MARKERS) < sum(m in text for m in MOJIBAKE_MARKERS):
                    text = repaired
                    break
            except Exception:
                pass

    return text


def normalize_compare_text(value) -> str:
    text = _safe_str(value) or ""
    text = fix_mojibake_text(text)
    text = text.strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = " ".join(text.split())
    return text


def repair_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    text_cols = out.select_dtypes(include=["object", "string"]).columns.tolist()
    for col in text_cols:
        out[col] = out[col].map(fix_mojibake_text)
    return out


def load_grupoareas_mapping(csv_path: str, encoding: str, sep: str) -> pd.DataFrame:
    path = Path(csv_path).expanduser()
    logger.info("Lendo planilha de grupo/área: %s", path)

    df = pd.read_csv(
        path,
        sep=sep,
        header=None,
        names=[
            "contrato_numero_csv",
            "novo_contrato_grupo_area",
            "novo_contrato_area",
        ],
        dtype="string",
        encoding=encoding,
    )

    # Repara textos da planilha também
    df = repair_text_columns(df)

    for col in df.columns:
        df[col] = df[col].astype("string").str.strip()

    df = df.dropna(subset=["contrato_numero_csv"])
    df["_contrato_numero_key"] = df["contrato_numero_csv"].map(normalize_compare_text)

    df = df.drop_duplicates(
        subset=["_contrato_numero_key"],
        keep="first",
    )

    logger.info("Planilha de grupo/área carregada com %s chaves únicas", len(df))
    return df


def apply_grupoareas_mapping(chunk: pd.DataFrame, mapping_df: pd.DataFrame, log_limit: int) -> pd.DataFrame:
    if chunk.empty:
        return chunk

    out = repair_text_columns(chunk)

    out["_contrato_numero_key"] = out["contrato_numero"].map(normalize_compare_text)

    out = out.merge(
        mapping_df[
            [
                "_contrato_numero_key",
                "novo_contrato_grupo_area",
                "novo_contrato_area",
            ]
        ],
        how="left",
        on=["_contrato_numero_key"],
    )

    group_mask = out["novo_contrato_grupo_area"].notna()
    area_mask = out["novo_contrato_area"].notna()
    matched_mask = group_mask | area_mask

    total_rows = len(out)
    total_matched = int(matched_mask.sum())
    total_group_changes = int(group_mask.sum())
    total_area_changes = int(area_mask.sum())

    logger.info(
        "Chunk SQL: %s linhas | Correspondências no CSV: %s | Trocas grupo: %s | Trocas área: %s",
        total_rows,
        total_matched,
        total_group_changes,
        total_area_changes,
    )

    if total_matched:
        examples = out.loc[matched_mask, [
            "contrato_numero",
            "contrato_grupo_area",
            "contrato_area",
            "novo_contrato_grupo_area",
            "novo_contrato_area",
        ]].head(log_limit)

        for _, row in examples.iterrows():
            logger.info(
                "MATCH | contrato=%s | grupo: %s -> %s | area: %s -> %s",
                row["contrato_numero"],
                row["contrato_grupo_area"],
                row["novo_contrato_grupo_area"],
                row["contrato_area"],
                row["novo_contrato_area"],
            )

        out.loc[group_mask, "contrato_grupo_area"] = out.loc[group_mask, "novo_contrato_grupo_area"]
        out.loc[area_mask, "contrato_area"] = out.loc[area_mask, "novo_contrato_area"]
    else:
        no_match_examples = out.loc[~matched_mask, [
            "contrato_numero", "contrato_grupo_area", "contrato_area"
        ]].head(log_limit)
        for _, row in no_match_examples.iterrows():
            logger.info(
                "SEM MATCH | contrato=%s | grupo=%s | area=%s",
                row["contrato_numero"],
                row["contrato_grupo_area"],
                row["contrato_area"],
            )

    out = out.drop(
        columns=[
            "_contrato_numero_key",
            "novo_contrato_grupo_area",
            "novo_contrato_area",
        ]
    )

    return out


def filter_cross_chunk_duplicates(chunk: pd.DataFrame, seen_hashes: set[int]) -> pd.DataFrame:
    if chunk.empty:
        return chunk

    hash_input = chunk.copy()
    for col in hash_input.columns:
        if hash_input[col].dtype.name in ("object", "string"):
            hash_input[col] = hash_input[col].fillna("<NULL>")
    hashes = pd.util.hash_pandas_object(hash_input, index=False)

    keep_mask = []
    removed = 0
    for h in hashes.tolist():
        if h in seen_hashes:
            keep_mask.append(False)
            removed += 1
        else:
            seen_hashes.add(h)
            keep_mask.append(True)

    if removed:
        logger.info("Duplicidades removidas entre chunks: %s", removed)

    return chunk.loc[keep_mask].copy()


def export_to_parquet(args: argparse.Namespace, out_path: Path):
    mapping_df = load_grupoareas_mapping(
        csv_path=args.grupoareas_csv,
        encoding=args.grupoareas_encoding,
        sep=args.grupoareas_sep,
    )

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        sslmode=args.sslmode,
    )

    logger.info("Executando consulta...")

    parquet_writer = None
    total = 0
    seen_hashes: set[int] = set()

    for chunk_idx, chunk in enumerate(
        iter_sql_chunks(
            SQL_SELECT,
            conn,
            params={"company_id": args.company_id, "project_names": args.project_names, "contratada_cnpjs": args.contratada_cnpjs},
            chunksize=args.chunksize,
        ),
        start=1,
    ):
        logger.info("Processando chunk %s com %s linhas...", chunk_idx, len(chunk))
        chunk = apply_grupoareas_mapping(chunk, mapping_df, args.grupoareas_log_limit)
        chunk = apply_contrato_mapping(chunk, args, classify_contratada=True)
        chunk = repair_text_columns(chunk)
        before_dedup = len(chunk)
        chunk = chunk.drop_duplicates().copy()
        removed_same_chunk = before_dedup - len(chunk)
        if removed_same_chunk:
            logger.info(
                "Duplicidades removidas dentro do chunk apos as trocas de grupo/area/contrato: %s",
                removed_same_chunk,
            )
        chunk = filter_cross_chunk_duplicates(chunk, seen_hashes)

        if chunk.empty:
            logger.info("Chunk %s ficou vazio após trocas/deduplicação; nada a gravar.", chunk_idx)
            continue

        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")

        parquet_writer.write_table(table)

        total += len(chunk)
        logger.info("Registros exportados acumulados: %s", total)

    if parquet_writer:
        parquet_writer.close()

    conn.close()

    logger.info("Arquivo parquet gerado com sucesso: %s", out_path)
    logger.info("Total final de registros: %s", total)


def main():
    args = parse_args()
    out_path = ensure_output_path(args.out)
    export_to_parquet(args, out_path)


if __name__ == "__main__":
    from migracao_logging import run_script

    raise SystemExit(run_script(main, __file__))
