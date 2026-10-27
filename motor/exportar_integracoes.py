#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from migracao_contrato_mapping import apply_contrato_mapping
from migracao_sql_export import iter_sql_chunks
from migracao_project_filter import CONTRATADAS_CTE, parse_project_args
import logging
from pathlib import Path

import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("export_integracoes")

# 06.INTEGRACOES (company_id parametrizado)
SQL_SELECT = "with " + CONTRATADAS_CTE + r"""
select distinct 
        TRN.contratante_cnpj
      , TRN.contrato_numero
      , TRN._contrato_project_id
      , TRN.contratada_identificador
      , TRN.colaborador_identificador
      , TRN.treinamento_apelido
      , TRN.treinamento_descricao
      , TRN.treinamento_endereco
      , TRN.treinamento_prazo_integracao
      , TRN.treinamento_horario_inicial      
      , TRN.treinamento_horario_final      
      , TRN.treinamento_dia_semana      
      , TRN.turma_data      
      , TRN.turma_descricao      
      , TRN.turma_quantidade_colaborador      
      , TRN.colaborador_aprovado      
      , TRN.colaborador_data_aprovacao      
      , TRN.colaborador_nota      
      , TRN.colaborador_motivo_reprovacao      
      , coalesce(ACC.id::text, 'migracao@bernhoeft.com.br') as aprovacao_autor_identificador
from (
select distinct 
       CTR.cnpj					as contratante_cnpj 
     , PRJ.name as contrato_numero
     , PRJ.id as _contrato_project_id
	 , PRV.id					as contratada_identificador
	 , EMP.id       			as colaborador_identificador
     , TCC.short_name   		as treinamento_apelido
     , TCC.name     			as treinamento_descricao
     , TCC.address        		as treinamento_endereco
     , coalesce(TCC.validity_in_months, 0)	as treinamento_prazo_integracao
     , to_char(TCC.start_hour, 'HH24:MI')	as treinamento_horario_inicial
     , to_char(TCC.end_hour, 'HH24:MI')		as treinamento_horario_final
	 , (select string_agg(
               case diasemana
                 when 'MONDAY' then '1'
                 when 'TUESDAY' then '2'
                 when 'WEDNESDAY' then '3'
                 when 'THURSDAY' then '4'
                 when 'FRIDAY' then '5'
                 when 'SATURDAY' then '6'
                 when 'SUNDAY' then '7'
               end, ',')
        from   unnest(tcc.week_days) as diasemana) as treinamento_dia_semana
	 , TRC.training_date	as turma_data
     , TCC.name				as turma_descricao
	 , TRC.vacancy_number   as turma_quantidade_colaborador
     , case TCE.status
         when 'APPROVED' then 1
         when 'NOT_EVALUATED' then null
         else 0
       end                 	as colaborador_aprovado
	 , case TCE.status
	     when 'APPROVED' then TCE.updated_at
		 else null
	   end					as colaborador_data_aprovacao
	 , TCE.score            as colaborador_nota
     , case TCE.status
         when 'DISAPPROVED' then 0
         when 'ABSENT' then 1
         else null
       end                    as colaborador_motivo_reprovacao
	 , (select TCL.editor_account_id 
		from training_class_action_log as TCL 
		where TCL.training_class_id = TRC.id 
		and   TCL.affected_employee_id = EMP.id
		and   TCL.action = 'UPDATED_EMPLOYEE_STATUS'
		and   TCL.new_employee_status = 'APPROVED'
	    order by TCL.id desc
	    limit 1) as aprovacao_autor_identificador
from   training_class_config as TCC
       inner join training_class as TRC 
               on TRC.training_class_config_id = TCC.id
              and TRC.deleted = false
       inner join training_class_employee as TCE
               on TCE.training_class_id = TRC.id
              and TCE.deleted = false
       inner join contract as CTT
               on CTT.id = TCE.contract_id
              and CTT.deleted = false
       inner join employee as EMP
               on EMP.id = CTT.employee_id
              and EMP.deleted = false
       inner join company as PRV
               on PRV.id = EMP.company_id
              and PRV.deleted = false
       inner join project as PRJ
               on PRJ.id = CTT.project_id
              and PRJ.deleted = false
       inner join company as CTR
	           on CTR.id = PRJ.company_id
       left  join training_class_file as TCF
               on TCF.training_class_id = TRC.id
              and TCF.deleted = false
where  PRJ.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF
      where CF.project_id = PRJ.id and CF.company_id = PRV.id
  ))
  and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))) as TRN
       left join  (select distinct
                            ACC.id
                    from account as ACC
                    inner join account_company as ACC_CPN
                            on ACC_CPN.account_id = ACC.id
                            and ACC_CPN.deleted = false
                    inner join company as CPN
                            on CPN.id = ACC_CPN.company_id
                            and CPN.id = %(company_id)s
                    where ACC.deleted = false
                    and not (ACC.active = false and ACC.uid is null)
                    union
                   select distinct
                            ACC.id
                    from account as ACC
                    inner join project_company_account as PCA
                            on PCA.account_id = ACC.id
                            and PCA.deleted = false
                    inner join project_company as PJC
                            on PJC.id = PCA.project_company_id
                            and PJC.deleted = false
                    inner join project as PRJ
                            on PRJ.id = PJC.project_id
                            and PRJ.deleted = false
                            and PRJ.company_id = %(company_id)s
                            and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
                    inner join company as CPN
                            on CPN.id = PRJ.company_id
                    where ACC.deleted = false
                    and not (ACC.active = false and ACC.uid is null)) as ACC
              on ACC.id = aprovacao_autor_identificador
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta INTEGRACOES para Parquet.")
    p.add_argument("--company-id", type=int, required=True)
    p.add_argument("--out", required=True, help="Arquivo parquet de saída")

    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--db", dest="dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", default=None, help="Opcional se usar .pgpass")
    p.add_argument("--sslmode", default="require")

    p.add_argument("--chunksize", type=int, default=50_000, help="Linhas por chunk (padrão: 50000)")
    p.add_argument("--compression", default="snappy", help="snappy|gzip|zstd|none")

    return parse_project_args(p)


def ensure_output_path(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    if path.is_dir():
        raise ValueError(f"--out precisa ser um arquivo (não um diretório): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def export_to_parquet(args: argparse.Namespace, out_path: Path) -> None:
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        sslmode=args.sslmode,
    )

    parquet_writer = None
    total = 0

    try:
        logger.info("Executando consulta (company_id=%s)...", args.company_id)

        for chunk in iter_sql_chunks(
            SQL_SELECT,
            conn,
            params={"company_id": args.company_id, "project_names": args.project_names, "contratada_cnpjs": args.contratada_cnpjs},
            chunksize=args.chunksize,
        ):
            chunk = apply_contrato_mapping(chunk, args)
            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if parquet_writer is None:
                compression = None if args.compression.lower() == "none" else args.compression.lower()
                parquet_writer = pq.ParquetWriter(out_path, table.schema, compression=compression)

            parquet_writer.write_table(table)
            total += len(chunk)
            logger.info("Registros exportados: %d", total)

        if parquet_writer is None:
            logger.warning("Consulta não retornou registros. Nenhum parquet foi gerado.")
        else:
            logger.info("Parquet gerado com sucesso: %s", out_path)

    finally:
        if parquet_writer:
            parquet_writer.close()
        conn.close()

    logger.info("Total de registros: %d", total)


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
