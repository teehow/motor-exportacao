#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from migracao_contrato_mapping import apply_contrato_mapping
from migracao_export_dates import adjust_export_datetimes
from migracao_sql_export import iter_sql_chunks
from migracao_project_filter import CONTRATADAS_CTE, parse_project_args
import logging
from pathlib import Path

import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("export_contratos")

SQL_SELECT = "with " + CONTRATADAS_CTE + r"""
select
		CPN.cnpj			as contratante_cnpj
	,	RTE.id				as colaborador_identificador
	,	RTE.document		as colaborador_cpf
	,	RTE.rg				as colaborador_rg
	,	null				as colaborador_passaporte
	,	null				as colaborador_pis
	,	RTE.name			as colaborador_nome
	,	RTE.foreign			as colaborador_estrangeiro
	,	0   				as colaborador_aposentado
	,	0					as colaborador_demitido
	,	null				as colaborador_data_demissao
	,	RTE.permanent_block	as colaborador_bloqueio_permanente
	,	RTE.block_reason	as colaborador_motivo_bloqueio
	,	RTE.created_at		as blacklist_data_bloqueio
	,	'migracao@bernhoeft.com.br'	as blacklist_autor_identificador
from restricted_employee RTE
	 inner join company as CPN
	 			on CPN.id = RTE.company_id
where RTE.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1
      from employee as EF
      inner join contract as CTF on CTF.employee_id = EF.id and CTF.deleted = false
      inner join project as PF on PF.id = CTF.project_id and PF.deleted = false
      inner join contratadas_filtradas as CF
          on CF.project_id = PF.id and CF.company_id = EF.company_id
      where EF.document = RTE.document and EF.deleted = false
        and PF.company_id = %(company_id)s
        and (%(project_names)s::text[] is null or PF.name = any(%(project_names)s::text[]))
  ))
  and (
      %(project_names)s::text[] is null
      or exists (
          select 1
          from employee as EMP
          inner join contract as CTT on CTT.employee_id = EMP.id and CTT.deleted = false
          inner join project as PRJ on PRJ.id = CTT.project_id and PRJ.deleted = false
          where EMP.document = RTE.document
            and EMP.deleted = false
            and PRJ.company_id = %(company_id)s
            and PRJ.name = any(%(project_names)s::text[])
      )
  )
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta contratos para Parquet.")
    p.add_argument("--company-id", type=int, required=True)

    p.add_argument("--out", required=True, help="Arquivo parquet de saída")

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


def export_to_parquet(args: argparse.Namespace, out_path: Path):

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        sslmode=args.sslmode,
    )

    logger.info("Executando consulta...")

    query = SQL_SELECT

    parquet_writer = None
    total = 0

    for chunk in iter_sql_chunks(
        query,
        conn,
        params={"company_id": args.company_id, "project_names": args.project_names, "contratada_cnpjs": args.contratada_cnpjs},
        chunksize=args.chunksize,
    ):

        chunk = apply_contrato_mapping(chunk, args)
        table = pa.Table.from_pandas(chunk)
        table = adjust_export_datetimes(table)

        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")

        parquet_writer.write_table(table)

        total += len(chunk)
        logger.info("Registros exportados: %s", total)

    if parquet_writer:
        parquet_writer.close()

    conn.close()

    logger.info("Arquivo parquet gerado com sucesso: %s", out_path)
    logger.info("Total de registros: %s", total)


def main():

    args = parse_args()
    out_path = ensure_output_path(args.out)

    export_to_parquet(args, out_path)


if __name__ == "__main__":
    from migracao_logging import run_script

    raise SystemExit(run_script(main, __file__))
