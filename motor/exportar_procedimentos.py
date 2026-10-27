#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from migracao_contrato_mapping import apply_contrato_mapping
from migracao_html_text import strip_html_text
from migracao_project_filter import CONTRATADAS_CTE, parse_project_args
import logging
import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import psycopg2


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("export_procedimento")


PARQUET_COLUMN_ORDER = [
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
]

DATE_COLS = [
    "competencia_data",
]

DATETIME_COLS = [
    "analise_data_analise",
]

INTEGER_COLS = [
    "contratada_identificador",
    "competencia_tipo",
    "analise_objeto_identificador",
    "procedimento_identificador",
]

STRING_COLS = [col for col in PARQUET_COLUMN_ORDER if col not in DATE_COLS + DATETIME_COLS + INTEGER_COLS]
PANDAS_TIMESTAMP_MIN = pd.Timestamp.min
PANDAS_TIMESTAMP_MAX = pd.Timestamp.max


SQL_SELECT = "with " + CONTRATADAS_CTE + r"""
-- Refatoracao da exportacao de procedimentos.
--
-- Mantem as mesmas regras usadas na exportacao de analises:
--   1. pendencias_base filtra PENDENCY e dados comuns uma unica vez.
--   2. pendencias_alvo resolve os alvos em ramos disjuntos:
--      colaborador direto, varios colaboradores ou empresa.
--   3. Pendencias por tipo de atividade so saem para colaboradores validos.
--   4. Calcula a data da analise uma vez por pendencia, antes dos procedimentos.
--      Usa checklist_log apos o corte e pendency_history antes dele.
--      Sem log/historico, usa as postagens como em analises; por ultimo, a criacao.
--   5. Nao ha DISTINCT final; duplicidade deve ser evitada na origem dos alvos.

, parametros as (
    select
        %(company_id)s::integer as company_id,
        date '2022-10-18' as data_corte,
        'migracao@bernhoeft.com.br'::text as usuario_migracao
),

pendencias_base as materialized (
    select
        PCY.id as pendencia_id,
        PCY.employee_id,
        PCY.plan_id,
        PCY.reference_month,
        PCY.created_at as pendencia_criada_em,
        PCY.pendency_id as analise_objeto_identificador,
        case when PCY.created_at >= PAR.data_corte then (
            select max(CKL.created_at)
            from pendency_checklist_log as CKL
            where CKL.pendency_id = PCY.id and CKL.deleted = false
        ) else (
            select max(HST.created_at)
            from pendency_history as HST
            where PCY.created_at < PAR.data_corte
              and HST.pendency_id = PCY.id and HST.deleted = false
              and HST.description in ('Mudou o status para: Não se aplica',
                                      'Mudou o status para: Concluído',
                                      'Mudou o status para: Pré-reprovado',
                                      'Mudou o status para: Em aberto')
        ) end as analise_data,
        case
            when exists (
                select 1 from document as DOC
                where DOC.pendency_id = PCY.id and DOC.deleted = false
            ) then 'ARQUIVO'
            when exists (
                select 1 from pendency_history as HST
                where HST.pendency_id = PCY.id and HST.deleted = false
                  and HST.message <> '' and HST.item_name is not null
            ) then 'JUSTIFICATIVA'
            else 'SEM ENTREGA'
        end as analise_objeto_tipo_entrega,

        SYP.type as system_pendency_type,
        case
            when SYP.type in ('SERVICE_PROVIDED', 'PROVIDER') then 'Empresa'
            when SYP.type in ('EMPLOYEE', 'RISK') then 'Colaborador'
        end as analise_objeto_tipo,

        PRJ.id as projeto_id,
        PRJ.name as contrato_numero,
        PRJ.start_date as projeto_data_inicio,
        PRJ.end_date as projeto_data_fim,

        CTR.cnpj as contratante_cnpj,
        CPN.id as contratada_identificador
    from parametros as PAR
         inner join project as PRJ
                 on PRJ.company_id = PAR.company_id
                and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
                and PRJ.deleted = false
         inner join company as CTR
                 on CTR.id = PRJ.company_id
         inner join pendency as PCY
                 on PCY.project_id = PRJ.id
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
         inner join system_pendency as SYP
                 on SYP.id = PCY.pendency_id
                and SYP.internal = false
                and SYP.pendency_category_id is not null
         inner join company as CPN
                 on CPN.id = PCY.provider_id
                 and (%(contratada_cnpjs)s::text[] is null or exists (
                     select 1 from contratadas_filtradas as CF
                     where CF.project_id = PRJ.id and CF.company_id = CPN.id
                 ))
                and CPN.deleted = false
         inner join plan as PLN
                 on PLN.id = PCY.plan_id
                and PLN.deleted = false
),

checklists_priorizados as materialized (
    select
        CKL.id,
        CKL.system_pendency_id,
        case
            when count(*) filter (where CKL.checklist_order = 1)
                 over (partition by CKL.system_pendency_id) > 0
                then coalesce(CKL.checklist_order = 1, false)
            else CKL.id = min(CKL.id) over (partition by CKL.system_pendency_id)
        end as procedimento_principal
    from system_checklist as CKL
    where CKL.deleted = false
      and exists (
          select 1 from pendencias_base as PB
          where PB.analise_objeto_identificador = CKL.system_pendency_id
      )
),

pendencias_alvo as (
    select
        PB.pendencia_id,
        PB.plan_id,
        PB.reference_month,
        PB.pendencia_criada_em,
        PB.analise_data,
        PB.analise_objeto_tipo_entrega,
        PB.analise_objeto_identificador,
        PB.analise_objeto_tipo,
        PB.contratante_cnpj,
        PB.contrato_numero,
        PB.projeto_id as _contrato_project_id,
        PB.contratada_identificador,
        EMP.id as employee_id,
        EMP.id::text as colaborador_identificador,
        case
            when REF.competencia_referencia < PB.projeto_data_inicio then PB.projeto_data_inicio
            when PB.projeto_data_fim is not null
             and REF.competencia_referencia > PB.projeto_data_fim then PB.projeto_data_fim
            else REF.competencia_referencia
        end as competencia_data
    from pendencias_base as PB
         inner join employee as EMP
                 on EMP.id = PB.employee_id
                and EMP.deleted = false
         inner join lateral (
             select CTT.created_at
             from contract as CTT
             where CTT.employee_id = EMP.id
               and CTT.project_id = PB.projeto_id
               and CTT.deleted = false
             order by CTT.created_at desc nulls last, CTT.id desc
             limit 1
         ) as CTT
                on true
         cross join lateral (
             select case
                 when PB.plan_id = 2 then coalesce(PB.reference_month,
                                                   date_trunc('month', PB.projeto_data_inicio))
                 else coalesce(CTT.created_at, PB.projeto_data_inicio)
             end as competencia_referencia
         ) as REF
    where not exists (
              select 1
              from system_pendency_activity_type as SPAT
              where SPAT.system_pendency_id = PB.analise_objeto_identificador
          )
       or (
              PB.plan_id = 2
          and exists (
              select 1
              from system_pendency_activity_type as SPAT
                   inner join employee_activity_type as EAT
                           on EAT.activity_type_id = SPAT.activity_type_id
                          and EAT.employee_id = EMP.id
                          and EAT.deleted = false
              where SPAT.system_pendency_id = PB.analise_objeto_identificador
                and EAT.activity_type_id not in (1, 35)
                and date_trunc('month', PB.reference_month) = date_trunc('month', EAT.movimentation_start_date)
          )
       )

    union all

    select
        PB.pendencia_id,
        PB.plan_id,
        PB.reference_month,
        PB.pendencia_criada_em,
        PB.analise_data,
        PB.analise_objeto_tipo_entrega,
        PB.analise_objeto_identificador,
        PB.analise_objeto_tipo,
        PB.contratante_cnpj,
        PB.contrato_numero,
        PB.projeto_id as _contrato_project_id,
        PB.contratada_identificador,
        EMP.id as employee_id,
        EMP.id::text as colaborador_identificador,
        case
            when REF.competencia_referencia < PB.projeto_data_inicio then PB.projeto_data_inicio
            when PB.projeto_data_fim is not null
             and REF.competencia_referencia > PB.projeto_data_fim then PB.projeto_data_fim
            else REF.competencia_referencia
        end as competencia_data
    from pendencias_base as PB
         inner join lateral (
             select distinct PCY_EMP.employee_id
             from pendency_employee as PCY_EMP
             where PCY_EMP.pendency_id = PB.pendencia_id
               and PCY_EMP.deleted = false
               and (PB.employee_id is null or PCY_EMP.employee_id <> PB.employee_id)
         ) as PE
                on true
         inner join employee as EMP
                 on EMP.id = PE.employee_id
                and EMP.deleted = false
         inner join lateral (
             select CTT.created_at
             from contract as CTT
             where CTT.employee_id = EMP.id
               and CTT.project_id = PB.projeto_id
               and CTT.deleted = false
             order by CTT.created_at desc nulls last, CTT.id desc
             limit 1
         ) as CTT
                on true
         cross join lateral (
             select case
                 when PB.plan_id = 2 then coalesce(PB.reference_month,
                                                   date_trunc('month', PB.projeto_data_inicio))
                 else coalesce(CTT.created_at, PB.projeto_data_inicio)
             end as competencia_referencia
         ) as REF
    where not exists (
              select 1
              from system_pendency_activity_type as SPAT
              where SPAT.system_pendency_id = PB.analise_objeto_identificador
          )
       or (
              PB.plan_id = 2
          and exists (
              select 1
              from system_pendency_activity_type as SPAT
                   inner join employee_activity_type as EAT
                           on EAT.activity_type_id = SPAT.activity_type_id
                          and EAT.employee_id = EMP.id
                          and EAT.deleted = false
              where SPAT.system_pendency_id = PB.analise_objeto_identificador
                and EAT.activity_type_id not in (1, 35)
                and date_trunc('month', PB.reference_month) = date_trunc('month', EAT.movimentation_start_date)
          )
       )

    union all

    select
        PB.pendencia_id,
        PB.plan_id,
        PB.reference_month,
        PB.pendencia_criada_em,
        PB.analise_data,
        PB.analise_objeto_tipo_entrega,
        PB.analise_objeto_identificador,
        PB.analise_objeto_tipo,
        PB.contratante_cnpj,
        PB.contrato_numero,
        PB.projeto_id as _contrato_project_id,
        PB.contratada_identificador,
        null::integer as employee_id,
        null::text as colaborador_identificador,
        case
            when PB.plan_id = 2 then
                case 
                    when PB.reference_month is null then
                        date_trunc('month',PB.projeto_data_inicio)
                    when PB.reference_month < PB.projeto_data_inicio then
                        date_trunc('month',PB.projeto_data_inicio)
                    when PB.reference_month > PB.projeto_data_fim then
                        date_trunc('month',PB.projeto_data_fim)
                    else
                        PB.reference_month
                end
            else 
                PB.projeto_data_inicio
        end as competencia_data 
    from pendencias_base as PB
    where PB.employee_id is null
      and PB.system_pendency_type in ('SERVICE_PROVIDED', 'PROVIDER')
      and not exists (
          select 1
          from pendency_employee as PCY_EMP
          where PCY_EMP.pendency_id = PB.pendencia_id
            and PCY_EMP.deleted = false
      )
      and not exists (
          select 1
          from system_pendency_activity_type as SPAT
          where SPAT.system_pendency_id = PB.analise_objeto_identificador
      )
)

select
    PA.contratante_cnpj,
    PA.contrato_numero,
    PA._contrato_project_id,
    PA.contratada_identificador,
    PA.colaborador_identificador,
    PA.competencia_data,
    case
        when PA.plan_id = 2 then 0
        else 1
    end as competencia_tipo,
    PA.analise_objeto_identificador,
    PA.analise_objeto_tipo,
    DAT.analise_data_analise,
    PAR.usuario_migracao as analise_autor_identificador,
    SYS_CKL.id as procedimento_identificador,
    case
        when CALC.procedimento_conforme <> 'NIL' then CALC.procedimento_conforme
        when PA.analise_objeto_tipo_entrega = 'SEM ENTREGA' and SYS_CKL.procedimento_principal then 'NÃO'
        when PA.analise_objeto_tipo_entrega = 'SEM ENTREGA' then 'N/A'
        when PA.analise_objeto_tipo_entrega <> 'SEM ENTREGA' then 'N/A'
        else CALC.procedimento_conforme
    end as procedimento_conforme,
    case
        when PA.analise_objeto_tipo_entrega = 'SEM ENTREGA'
         and CALC.procedimento_conforme = 'NIL' and SYS_CKL.procedimento_principal
            then 'O documento não foi apresentado (Migração BexUp).'
        else trim(split_part(PCY_CKL.invalid_description, 'RECOMENDAÇÃO:', 1))
    end as procedimento_comentario,
    case
        when PA.analise_objeto_tipo_entrega = 'SEM ENTREGA'
         and CALC.procedimento_conforme = 'NIL' and SYS_CKL.procedimento_principal
            then 'Deverá ser apresentado o documento.'
        when PCY_CKL.invalid_description like '%%RECOMENDAÇÃO:%%'
            then split_part(PCY_CKL.invalid_description, 'RECOMENDAÇÃO:', 2)
        else null
    end as procedimento_recomendacao
from pendencias_alvo as PA
     cross join parametros as PAR
     cross join lateral (
         -- So consulta postagens quando nao existe data de analise no log/historico.
         with documentos_pendencia as materialized (
             select DOC.created_at
             from document as DOC
             where PA.analise_data is null
               and DOC.pendency_id = PA.pendencia_id and DOC.deleted = false
         ),
         justificativas_pendencia as materialized (
             select HST.created_at
             from pendency_history as HST
             where PA.analise_data is null
               and HST.pendency_id = PA.pendencia_id
               and HST.deleted = false and HST.message <> '' and HST.item_name is not null
               and not exists (select 1 from documentos_pendencia)
         )
         select distinct
             coalesce(PA.analise_data, RES.data_postagem_resposta, PA.pendencia_criada_em) as analise_data_analise
         from (
             select DOC.created_at as data_postagem_resposta
             from documentos_pendencia as DOC
             union all
             select HST.created_at
             from justificativas_pendencia as HST
             union all
             select null::timestamp
             where not exists (select 1 from documentos_pendencia)
               and not exists (select 1 from justificativas_pendencia)
         ) as RES
     ) as DAT
     inner join checklists_priorizados as SYS_CKL
             on SYS_CKL.system_pendency_id = PA.analise_objeto_identificador
     left join pendency_checklist as PCY_CKL
             on PCY_CKL.pendency_id = PA.pendencia_id
            and PCY_CKL.system_checklist_id = SYS_CKL.id
            and PCY_CKL.deleted = false
     cross join lateral (
         select case PCY_CKL.status
             when 'OK' then 'SIM'
             when 'INVALID' then 'NÃO'
             when 'NOT_APPLICABLE' then 'N/A'
             else 'NIL'
         end as procedimento_conforme
     ) as CALC
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Exporta análise automática do PostgreSQL para Parquet."
    )
    p.add_argument("--company-id", type=int, required=True, help="ID da company")
    p.add_argument("--out", required=True, help="Caminho completo do arquivo .parquet")

    p.add_argument("--host", required=True, help="Host do PostgreSQL")
    p.add_argument("--port", type=int, default=5432, help="Porta do PostgreSQL")
    p.add_argument("--db", dest="dbname", required=True, help="Nome do banco")
    p.add_argument("--user", required=True, help="Usuário")
    p.add_argument("--password", default=None, help="Senha opcional; se omitida, tenta .pgpass")
    p.add_argument(
        "--sslmode",
        default="require",
        help="disable|allow|prefer|required|verify-ca|verify-full"
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Quantidade de linhas por lote na leitura do PostgreSQL e escrita do Parquet.",
    )
    p.add_argument(
        "--parquet-compression",
        default="snappy",
        help="Compressao do Parquet usada pelo pyarrow. Exemplos: snappy, gzip, zstd ou none.",
    )

    return parse_project_args(p)


def ensure_output_path(path_str: str) -> Path:
    out_path = Path(path_str).expanduser()
    if out_path.is_dir():
        raise ValueError(f"--out precisa apontar para um arquivo, não diretório: {out_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path


def get_pgpass_path() -> Path | None:
    appdata = os.getenv("APPDATA")
    if not appdata:
        return None
    return Path(appdata) / "postgresql" / "pgpass.conf"


def build_connect_kwargs(args: argparse.Namespace) -> dict:
    kwargs = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "sslmode": args.sslmode,
    }

    if args.password is not None:
        kwargs["password"] = args.password
        logger.info("Usando senha informada por parâmetro.")
        return kwargs

    pgpass_path = get_pgpass_path()
    if pgpass_path and pgpass_path.exists():
        logger.info("Senha não informada. Tentando usar pgpass: %s", pgpass_path)
    else:
        logger.info("Senha não informada. Tentando usar pgpass/ambiente padrão do PostgreSQL.")

    return kwargs


def load_pyarrow_modules():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Exportacao em lotes para Parquet requer o pacote pyarrow. "
            "Instale pyarrow no ambiente que executa este script."
        ) from exc

    return pa, pq


def build_parquet_schema(pa):
    return pa.schema([
        ("contratante_cnpj", pa.string()),
        ("contrato_numero", pa.string()),
        ("contratada_identificador", pa.int64()),
        ("colaborador_identificador", pa.string()),
        ("competencia_data", pa.date32()),
        ("competencia_tipo", pa.int64()),
        ("analise_objeto_identificador", pa.int64()),
        ("analise_objeto_tipo", pa.string()),
        ("analise_data_analise", pa.timestamp("ns")),
        ("analise_autor_identificador", pa.string()),
        ("procedimento_identificador", pa.int64()),
        ("procedimento_conforme", pa.string()),
        ("procedimento_comentario", pa.string()),
        ("procedimento_recomendacao", pa.string()),
    ])


def normalize_datetime_value(value):
    if value is None:
        return pd.NaT

    try:
        if pd.isna(value):
            return pd.NaT
    except (TypeError, ValueError):
        pass

    try:
        timestamp = pd.Timestamp(value)
    except (OverflowError, TypeError, ValueError):
        return pd.NaT

    if timestamp < PANDAS_TIMESTAMP_MIN or timestamp > PANDAS_TIMESTAMP_MAX:
        return pd.NaT

    return timestamp


def normalize_datetime_column(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.map(normalize_datetime_value), errors="coerce")


def normalize_date_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    if isinstance(value, str):
        text = value.strip()
        if len(text) >= 10:
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                pass

    try:
        timestamp = pd.Timestamp(value)
    except (OverflowError, TypeError, ValueError):
        return None

    if pd.isna(timestamp):
        return None

    return timestamp.date()


def normalize_date_column(series: pd.Series) -> pd.Series:
    return series.map(normalize_date_value)


def normalize_batch(df: pd.DataFrame) -> pd.DataFrame:
    missing_cols = [col for col in PARQUET_COLUMN_ORDER if col not in df.columns]
    if missing_cols:
        raise RuntimeError(f"Query nao retornou colunas esperadas: {missing_cols}")

    df = df[PARQUET_COLUMN_ORDER].copy()

    for col in DATE_COLS:
        df[col] = normalize_date_column(df[col])
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise RuntimeError(f"Coluna {col} retornou {null_count} valor(es) nulo(s), mas o campo e obrigatorio.")

    for col in DATETIME_COLS:
        before_nulls = df[col].isna().sum()
        df[col] = normalize_datetime_column(df[col])
        invalid_count = df[col].isna().sum() - before_nulls
        if invalid_count > 0:
            logger.warning(
                "Coluna %s: %s data(s) fora do intervalo do Parquet foram gravadas como nulo.",
                col,
                invalid_count,
            )

    for col in INTEGER_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in STRING_COLS:
        df[col] = df[col].astype("string")

    for col in ("procedimento_comentario", "procedimento_recomendacao"):
        df[col] = df[col].map(strip_html_text).astype("string")

    return df


def empty_table(pa, schema):
    return pa.Table.from_pydict(
        {field.name: pa.array([], type=field.type) for field in schema},
        schema=schema,
    )


def export_to_parquet(args: argparse.Namespace, out_path: Path) -> None:
    if args.batch_size <= 0:
        raise ValueError("--batch-size precisa ser maior que zero.")

    connect_kwargs = build_connect_kwargs(args)
    pa, pq = load_pyarrow_modules()
    schema = build_parquet_schema(pa)
    compression = None if str(args.parquet_compression).lower() == "none" else args.parquet_compression
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")
    sql_select = SQL_SELECT.strip().rstrip(";")

    if tmp_path.exists():
        tmp_path.unlink()

    writer = None
    total_rows = 0

    try:
        logger.info("Conectando no PostgreSQL...")
        with psycopg2.connect(**connect_kwargs) as conn:
            conn.set_session(readonly=True)
            cursor_name = f"export_procedimentos_{os.getpid()}"

            logger.info(
                "Executando query para company_id=%s em lotes de %s linhas...",
                args.company_id,
                args.batch_size,
            )
            with conn.cursor(name=cursor_name) as cur:
                cur.itersize = args.batch_size
                cur.execute(sql_select, {"company_id": args.company_id, "project_names": args.project_names, "pendency_ids": args.pendency_ids, "contratada_cnpjs": args.contratada_cnpjs, "considerar_inactive": args.considerar_inactive})

                writer = pq.ParquetWriter(tmp_path, schema, compression=compression)

                while True:
                    rows = cur.fetchmany(args.batch_size)
                    if not rows:
                        break

                    cols = [desc[0] for desc in cur.description]
                    df = pd.DataFrame.from_records(rows, columns=cols)
                    df = apply_contrato_mapping(df, args)
                    df = normalize_batch(df)
                    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
                    writer.write_table(table)

                    total_rows += len(df)
                    logger.info("Lote gravado: %s linhas; total acumulado: %s", len(df), total_rows)

        if writer is not None and total_rows == 0:
            writer.write_table(empty_table(pa, schema))

    except Exception:
        if writer is not None:
            writer.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    else:
        if writer is not None:
            writer.close()
        tmp_path.replace(out_path)

    logger.info("Linhas retornadas: %s", total_rows)
    logger.info("Arquivo gerado com sucesso: %s", out_path)

def main() -> int:
    try:
        args = parse_args()
        out_path = ensure_output_path(args.out)
        export_to_parquet(args, out_path)
        return 0
    except Exception as e:
        logger.exception("Falha: %s", e)
        return 1


if __name__ == "__main__":
    from migracao_logging import run_script

    raise SystemExit(run_script(main, __file__))
