#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from migracao_contrato_mapping import apply_contrato_mapping
from migracao_project_filter import CONTRATADAS_CTE, parse_project_args
import logging
import os
from datetime import date, datetime
from pathlib import Path
from time import perf_counter

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("export_analises")


PARQUET_COLUMN_ORDER = [
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
]

DATE_COLS = [
    "competencia_data",
]

DATETIME_COLS = [
    "analise_data_analise",
    "documento_data_emissao",
    "documento_data_validade",
    "documento_data_postagem",
    "justificativa_data_postagem",
]

INTEGER_COLS = [
    "contratada_identificador",
    "competencia_tipo",
    "analise_objeto_identificador",
    "documento_identificador",
    "documento_tamanho_arquivo",
    "justificativa_identificador",
]

STRING_COLS = [col for col in PARQUET_COLUMN_ORDER if col not in DATE_COLS + DATETIME_COLS + INTEGER_COLS]
PANDAS_TIMESTAMP_MIN = pd.Timestamp.min
PANDAS_TIMESTAMP_MAX = pd.Timestamp.max


SQL_SELECT = r"""
with parametros as not materialized (
    select
        %(company_id)s::integer as company_id,
        %(project_ids)s::integer[] as project_ids,
        date '2022-10-18' as data_corte,
        'migracao@bernhoeft.com.br'::text as usuario_migracao
),
projetos_filtrados as materialized (
    select PRJ.id, PRJ.company_id, PRJ.name, PRJ.start_date, PRJ.end_date
    from project as PRJ
    where PRJ.company_id = %(company_id)s
      and PRJ.deleted = false
      and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
      and (%(project_ids)s::integer[] is null or PRJ.id = any(%(project_ids)s::integer[]))
),
contratadas_filtradas as materialized (
    select distinct PCF.project_id, PCF.company_id
    from projetos_filtrados as PF
    inner join project_company as PCF on PCF.project_id = PF.id and PCF.deleted = false
    inner join company as CF on CF.id = PCF.company_id and CF.deleted = false
    where %(contratada_cnpjs)s::text[] is not null
      and regexp_replace(CF.cnpj, '[^0-9]', '', 'g') = any(%(contratada_cnpjs)s::text[])
),

tipos_atividade_sistema as (
    select distinct
        SPAT.system_pendency_id
    from system_pendency_activity_type as SPAT
),

pendencias_base as materialized (
    select
        PCY.id as pendencia_id,
        PCY.employee_id,
        PCY.plan_id,
        PCY.reference_month,
        PCY.created_at as pendencia_criada_em,
        PCY.status as pendencia_status,
        PCY.expiration_date as documento_data_validade,
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

        SYP.type as system_pendency_type,
        TAT.system_pendency_id is not null as possui_tipo_atividade,
        case
            when SYP.type in ('SERVICE_PROVIDED', 'PROVIDER') then 'Empresa'
            when SYP.type in ('EMPLOYEE', 'RISK') then 'Colaborador'
        end as analise_objeto_tipo,
        SYP.name as analise_objeto_descricao,

        PRJ.id as projeto_id,
        PRJ.name as contrato_numero,
        PRJ.start_date as projeto_data_inicio,
        PRJ.end_date as projeto_data_fim,

        CTR.cnpj as contratante_cnpj,
        CPN.id as contratada_identificador
    from projetos_filtrados as PRJ
         cross join parametros as PAR
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
          left join tipos_atividade_sistema as TAT
                 on TAT.system_pendency_id = SYP.id
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

pendencias_colaboradores as (
    select distinct
        PCY_EMP.pendency_id,
        PCY_EMP.employee_id
    from pendencias_base as PB
         inner join pendency_employee as PCY_EMP
                 on PCY_EMP.pendency_id = PB.pendencia_id
                and PCY_EMP.deleted = false
),

alvos_colaborador_base as (
    select
        PB.projeto_id,
        PB.employee_id
    from pendencias_base as PB
    where PB.employee_id is not null

    union

    select
        PB.projeto_id,
        PE.employee_id
    from pendencias_base as PB
         inner join pendencias_colaboradores as PE
                 on PE.pendency_id = PB.pendencia_id
    where PE.employee_id is not null
),

contratos_colaborador as (
    select distinct on (CTT.employee_id, CTT.project_id)
        CTT.employee_id,
        CTT.project_id,
        CTT.created_at
    from contract as CTT
         inner join alvos_colaborador_base as ACB
                 on ACB.employee_id = CTT.employee_id
                and ACB.projeto_id = CTT.project_id
    where CTT.deleted = false
    order by CTT.employee_id, CTT.project_id, CTT.created_at desc nulls last, CTT.id desc
),

pendencias_alvo as not materialized (
    select
        PB.pendencia_id,
        PB.plan_id,
        PB.pendencia_criada_em,
        PB.pendencia_status,
        PB.analise_data,
        PB.documento_data_validade,
        PB.analise_objeto_identificador,
        PB.analise_objeto_tipo,
        PB.analise_objeto_descricao,
        PB.contratante_cnpj,
        PB.contrato_numero,
        PB.projeto_id as _contrato_project_id,
        PB.contratada_identificador,
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
         inner join contratos_colaborador as CTT
                 on CTT.employee_id = EMP.id
                and CTT.project_id = PB.projeto_id
         cross join lateral (
             select case
                 when PB.plan_id = 2 then coalesce(PB.reference_month,
                                                    date_trunc('month', PB.projeto_data_inicio))
                 else coalesce(CTT.created_at, PB.projeto_data_inicio)
             end as competencia_referencia
         ) as REF
    where PB.possui_tipo_atividade = false
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
                and EAT.movimentation_start_date >= date_trunc('month', PB.reference_month)
                and EAT.movimentation_start_date < date_trunc('month', PB.reference_month) + interval '1 month'
          )
       )

    union all

    select
        PB.pendencia_id,
        PB.plan_id,
        PB.pendencia_criada_em,
        PB.pendencia_status,
        PB.analise_data,
        PB.documento_data_validade,
        PB.analise_objeto_identificador,
        PB.analise_objeto_tipo,
        PB.analise_objeto_descricao,
        PB.contratante_cnpj,
        PB.contrato_numero,
        PB.projeto_id as _contrato_project_id,
        PB.contratada_identificador,
        EMP.id::text as colaborador_identificador,
        case
            when REF.competencia_referencia < PB.projeto_data_inicio then PB.projeto_data_inicio
            when PB.projeto_data_fim is not null
             and REF.competencia_referencia > PB.projeto_data_fim then PB.projeto_data_fim
            else REF.competencia_referencia
        end as competencia_data
    from pendencias_base as PB
         inner join pendencias_colaboradores as PE
                 on PE.pendency_id = PB.pendencia_id
                and (PB.employee_id is null or PE.employee_id <> PB.employee_id)
         inner join employee as EMP
                 on EMP.id = PE.employee_id
                and EMP.deleted = false
         inner join contratos_colaborador as CTT
                 on CTT.employee_id = EMP.id
                and CTT.project_id = PB.projeto_id
         cross join lateral (
             select case
                 when PB.plan_id = 2 then coalesce(PB.reference_month,
                                                    date_trunc('month', PB.projeto_data_inicio))
                 else coalesce(CTT.created_at, PB.projeto_data_inicio)
             end as competencia_referencia
         ) as REF
    where PB.possui_tipo_atividade = false
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
                and EAT.movimentation_start_date >= date_trunc('month', PB.reference_month)
                and EAT.movimentation_start_date < date_trunc('month', PB.reference_month) + interval '1 month'
          )
       )

    union all

    select
        PB.pendencia_id,
        PB.plan_id,
        PB.pendencia_criada_em,
        PB.pendencia_status,
        PB.analise_data,
        PB.documento_data_validade,
        PB.analise_objeto_identificador,
        PB.analise_objeto_tipo,
        PB.analise_objeto_descricao,
        PB.contratante_cnpj,
        PB.contrato_numero,
        PB.projeto_id as _contrato_project_id,
        PB.contratada_identificador,
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
          from pendencias_colaboradores as PE
          where PE.pendency_id = PB.pendencia_id
      )
      and PB.possui_tipo_atividade = false
)

select
    PA.contratante_cnpj,
    PA.contrato_numero,
    PA._contrato_project_id,
    PA.contratada_identificador,
    PA.colaborador_identificador,
    case 
        when PA.plan_id = 2 then date_trunc('month', PA.competencia_data)
        else PA.competencia_data
    end as competencia_data,
    case
        when PA.plan_id = 2 then 0
        else 1
    end as competencia_tipo,
    PA.analise_objeto_identificador,
    PA.analise_objeto_tipo,
    PA.analise_objeto_descricao,
    case
        when RES.analise_objeto_tipo_entrega = 'ARQUIVO'
         and nullif(trim(RES.documento_revisao), '') is null then 'NIL'
        else 'N/A'
    end as analise_objeto_conforme,
    RES.analise_objeto_tipo_entrega,
    coalesce(PA.analise_data, RES.data_postagem_resposta, PA.pendencia_criada_em) as analise_data_analise,
    PAR.usuario_migracao as analise_autor_identificador,
    RES.documento_identificador,
    RES.documento_nome_arquivo,
    RES.documento_caminho_storage,
    RES.documento_tamanho_arquivo,
    case when PA.pendencia_status = 'NOT_APPLICABLE' then null
         else RES.documento_data_emissao end as documento_data_emissao,
    case when PA.pendencia_status = 'NOT_APPLICABLE' then null
         else PA.documento_data_validade end as documento_data_validade,
    RES.documento_data_postagem,
    RES.documento_autor_identificador,
    RES.justificativa_identificador,
    RES.justificativa_texto,
    RES.justificativa_data_postagem,
    RES.justificativa_autor_identificador
from pendencias_alvo as PA
     cross join parametros as PAR
     inner join lateral (
         -- Estes conjuntos ficam restritos a uma pendencia, nao ao bloco inteiro.
         with documentos_pendencia as materialized (
             select DOC.pendency_id as pendencia_id, DOC.id, DOC.filename, DOC.bucket_doc_id,
                    DOC.size, DOC.issue_date, DOC.created_at, DOC.document_review
             from document as DOC
             where DOC.pendency_id = PA.pendencia_id and DOC.deleted = false
         ),
         justificativas_pendencia as materialized (
             select HST.pendency_id as pendencia_id, HST.id, HST.message, HST.created_at
             from pendency_history as HST
             where HST.pendency_id = PA.pendencia_id
               and HST.deleted = false and HST.message <> '' and HST.item_name is not null
               and not exists (select 1 from documentos_pendencia)
         )
         select DOC.pendencia_id,
                'ARQUIVO'::text as analise_objeto_tipo_entrega,
                DOC.created_at as data_postagem_resposta,
                DOC.id as documento_identificador,
                DOC.filename as documento_nome_arquivo,
                DOC.bucket_doc_id::text as documento_caminho_storage,
                DOC.size as documento_tamanho_arquivo,
                DOC.issue_date as documento_data_emissao,
                DOC.created_at as documento_data_postagem,
                PAR.usuario_migracao as documento_autor_identificador,
                null::integer as justificativa_identificador,
                null::text as justificativa_texto,
                null::timestamp as justificativa_data_postagem,
                null::text as justificativa_autor_identificador,
                DOC.document_review::text as documento_revisao
         from documentos_pendencia as DOC

         union all

         select HST.pendencia_id, 'JUSTIFICATIVA'::text, HST.created_at,
                null::integer, null::text, null::text, null::integer, null::date,
                null::timestamp, null::text,
                HST.id, HST.message, HST.created_at, PAR.usuario_migracao, null::text
         from justificativas_pendencia as HST

         union all

         select PA.pendencia_id, 'SEM ENTREGA'::text, null::timestamp,
                null::integer, null::text, null::text, null::integer, null::date,
                null::timestamp, null::text,
                null::integer, null::text, null::timestamp, null::text, null::text
         where not exists (select 1 from documentos_pendencia)
           and not exists (select 1 from justificativas_pendencia)
     ) as RES on true
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
order by PRJ.id
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta análises do PostgreSQL para Parquet.")
    p.add_argument("--company-id", type=int, required=True, help="ID da company")
    p.add_argument("--out", required=True, help="Caminho completo do arquivo .parquet")

    p.add_argument("--host", required=True, help="Host do PostgreSQL")
    p.add_argument("--port", type=int, default=5432, help="Porta do PostgreSQL")
    p.add_argument("--db", dest="dbname", required=True, help="Nome do banco")
    p.add_argument("--user", required=True, help="Usuário")
    p.add_argument("--password", default=None, help="Senha opcional; se omitida, tenta .pgpass")
    p.add_argument("--sslmode", default="require", help="disable|allow|prefer|required|verify-ca|verify-full")
    p.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Quantidade maxima de linhas por lote de conversao e escrita no Parquet.",
    )
    p.add_argument(
        "--fetch-size",
        type=int,
        default=5000,
        help="Quantidade maxima de linhas por busca no PostgreSQL, acumuladas ate atingir --batch-size.",
    )
    p.add_argument(
        "--project-block-size",
        type=int,
        default=int(os.getenv("MIGRACAO_ANALISES_PROJECT_BLOCK_SIZE", "50")),
        help="Quantidade de projetos por bloco na geracao de analises. Use 0 para executar a empresa inteira em uma unica query.",
    )
    p.add_argument(
        "--parquet-compression",
        default="snappy",
        help="Compressão do Parquet usada pelo pyarrow. Exemplos: snappy, gzip, zstd ou none.",
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

    # Se senha foi informada, usa normalmente.
    if args.password is not None:
        kwargs["password"] = args.password
        logger.info("Usando senha informada por parâmetro.")
        return kwargs

    # Sem senha: deixa o psycopg2 usar .pgpass / variáveis de ambiente
    pgpass_path = get_pgpass_path()
    if pgpass_path and pgpass_path.exists():
        logger.info("Senha não informada. Tentando usar pgpass: %s", pgpass_path)
    else:
        logger.info("Senha não informada. Tentando usar pgpass/ambiente padrão do PostgreSQL.")

    return kwargs


def load_psycopg2_module():
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "Conexão com PostgreSQL requer o pacote psycopg2. "
            "Instale psycopg2 no ambiente que executa este script."
        ) from exc

    return psycopg2


def load_pyarrow_modules():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Exportação em lotes para Parquet requer o pacote pyarrow. "
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
        ("analise_objeto_descricao", pa.string()),
        ("analise_objeto_conforme", pa.string()),
        ("analise_objeto_tipo_entrega", pa.string()),
        ("analise_data_analise", pa.timestamp("ns")),
        ("analise_autor_identificador", pa.string()),
        ("documento_identificador", pa.int64()),
        ("documento_nome_arquivo", pa.string()),
        ("documento_caminho_storage", pa.string()),
        ("documento_tamanho_arquivo", pa.int64()),
        ("documento_data_emissao", pa.timestamp("ns")),
        ("documento_data_validade", pa.timestamp("ns")),
        ("documento_data_postagem", pa.timestamp("ns")),
        ("documento_autor_identificador", pa.string()),
        ("justificativa_identificador", pa.int64()),
        ("justificativa_texto", pa.string()),
        ("justificativa_data_postagem", pa.timestamp("ns")),
        ("justificativa_autor_identificador", pa.string()),
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
    if pd.api.types.is_datetime64_any_dtype(series.dtype) and getattr(series.dtype, "tz", None) is None:
        converted = pd.to_datetime(series, errors="coerce")
        return converted.where(converted.between(PANDAS_TIMESTAMP_MIN, PANDAS_TIMESTAMP_MAX))
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
        raise RuntimeError(f"Query n?o retornou colunas esperadas: {missing_cols}")

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

    return df

def empty_table(pa, schema):
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


def export_to_parquet(args: argparse.Namespace, out_path: Path) -> None:
    if args.batch_size <= 0:
        raise ValueError("--batch-size precisa ser maior que zero.")
    if args.fetch_size <= 0:
        raise ValueError("--fetch-size precisa ser maior que zero.")
    if args.project_block_size < 0:
        raise ValueError("--project-block-size precisa ser maior ou igual a zero.")

    psycopg2 = load_psycopg2_module()
    pa, pq = load_pyarrow_modules()
    schema = build_parquet_schema(pa)
    connect_kwargs = build_connect_kwargs(args)
    compression = None if str(args.parquet_compression).lower() == "none" else args.parquet_compression
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")

    if tmp_path.exists():
        tmp_path.unlink()

    writer = None
    total_rows = 0
    fetch_size = min(args.batch_size, args.fetch_size)
    sql_select = SQL_SELECT.strip().rstrip(";")
    batch_rows = []
    batch_columns = None
    timings = dict(leitura=0.0, dataframe=0.0, normalizacao=0.0, arrow=0.0, escrita=0.0)

    def write_batch(rows, columns):
        nonlocal total_rows
        started = perf_counter()
        df = pd.DataFrame.from_records(rows, columns=columns)
        df = apply_contrato_mapping(df, args)
        after_dataframe = perf_counter()
        df = normalize_batch(df)
        after_normalization = perf_counter()
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        after_arrow = perf_counter()
        writer.write_table(table)
        finished = perf_counter()
        timings["dataframe"] += after_dataframe - started
        timings["normalizacao"] += after_normalization - after_dataframe
        timings["arrow"] += after_arrow - after_normalization
        timings["escrita"] += finished - after_arrow
        total_rows += len(df)
        logger.info(
            "Lote gravado: %s linhas; total: %s; dataframe: %.3fs; normalizacao: %.3fs; Arrow: %.3fs; escrita: %.3fs",
            len(df), total_rows, after_dataframe - started,
            after_normalization - after_dataframe, after_arrow - after_normalization,
            finished - after_arrow,
        )

    try:
        logger.info("Conectando no PostgreSQL...")
        with psycopg2.connect(**connect_kwargs) as conn:
            conn.set_session(readonly=True)
            project_blocks = build_project_blocks(conn, args)

            if args.project_block_size > 0:
                logger.info(
                    "Executando query para company_id=%s em %s bloco(s) de ate %s projeto(s), com fetch de ate %s linhas...",
                    args.company_id,
                    len(project_blocks),
                    args.project_block_size,
                    fetch_size,
                )
            else:
                logger.info(
                    "Executando query para company_id=%s em lote unico, com fetch de ate %s linhas...",
                    args.company_id,
                    fetch_size,
                )

            writer = pq.ParquetWriter(tmp_path, schema, compression=compression)
            for block_index, project_ids in enumerate(project_blocks, start=1):
                block_started = perf_counter()
                block_read_started = timings["leitura"]
                cursor_name = f"export_analises_{os.getpid()}_{block_index}"
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

                with conn.cursor(name=cursor_name) as cur:
                    cur.itersize = fetch_size
                    cur.execute(sql_select, params)

                    block_rows = 0
                    while True:
                        started = perf_counter()
                        rows = cur.fetchmany(fetch_size)
                        read_seconds = perf_counter() - started
                        timings["leitura"] += read_seconds
                        logger.info(
                            "Busca PostgreSQL: bloco %s/%s; %s linhas; %.3fs",
                            block_index, len(project_blocks), len(rows), read_seconds,
                        )
                        if not rows:
                            break

                        cols = [desc[0] for desc in cur.description]
                        if batch_columns is None:
                            batch_columns = cols
                        elif cols != batch_columns:
                            raise RuntimeError("As colunas retornadas mudaram entre os blocos da exportacao.")
                        block_rows += len(rows)
                        # A busca e a escrita sao independentes; o buffer continua entre projetos.
                        offset = 0
                        while offset < len(rows):
                            count = min(args.batch_size - len(batch_rows), len(rows) - offset)
                            batch_rows.extend(rows[offset:offset + count])
                            offset += count
                            if len(batch_rows) == args.batch_size:
                                write_batch(batch_rows, batch_columns)
                                batch_rows.clear()

                logger.info(
                    "Bloco %s/%s lido: %s linhas; total: %.3fs; buscas PostgreSQL: %.3fs",
                    block_index, len(project_blocks), block_rows,
                    perf_counter() - block_started, timings["leitura"] - block_read_started,
                )

            if batch_rows:
                write_batch(batch_rows, batch_columns)
                batch_rows.clear()

            if not project_blocks:
                logger.info("Nenhum projeto com pendencias encontrado para company_id=%s.", args.company_id)

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
    logger.info(
        "Tempos acumulados: leitura PostgreSQL: %.3fs; dataframe: %.3fs; normalizacao: %.3fs; Arrow: %.3fs; escrita Parquet: %.3fs",
        timings["leitura"], timings["dataframe"], timings["normalizacao"], timings["arrow"], timings["escrita"],
    )
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
