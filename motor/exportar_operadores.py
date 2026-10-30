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
logger = logging.getLogger("export_usuarios")

# Query parametrizada por company_id
SQL_SELECT = "with " + CONTRATADAS_CTE + r"""
select  contratante_cnpj
      , operador_identificador
      , operador_cpf
      , operador_nome
      , operador_fone_fixo
      , operador_fone_celular
      , operador_observacoes
      , operador_email
      , operador_data_inclusao
      , operador_responsavel_nome
      , operador_responsavel_email
      , operador_responsavel_fone
      , min (operador_perfil) as operador_perfil
from (
select distinct
        CPN.cnpj		as contratante_cnpj
	  , ACC.id			as operador_identificador
	  , null			as operador_cpf
      , ACC.name        as operador_nome
      , '3199999999'    as operador_fone_fixo
      , null            as operador_fone_celular
      , null            as operador_observacoes
      , ACC.email		as operador_email
      , ACC.created_at	as operador_data_inclusao
      , null			as operador_responsavel_nome
      , null			as operador_responsavel_email
      , null			as operador_responsavel_fone
      , 'CLIENTE'       as operador_perfil
from account as ACC
     inner join account_company as ACC_CPN
                 on ACC_CPN.account_id = ACC.id
                and ACC_CPN.deleted = false
     inner join company as CPN
                 on CPN.id = ACC_CPN.company_id
                and CPN.id = %(company_id)s
where ACC.deleted = false
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF
      inner join project as PF on PF.id = CF.project_id and PF.deleted = false
      where PF.company_id = %(company_id)s
        and (%(project_names)s::text[] is null or PF.name = any(%(project_names)s::text[]))
  ))
  and not (ACC.active = false and ACC.uid is null)
union
select distinct
        CPN.cnpj		as contratante_cnpj
	  , ACC.id			as operador_identificador
	  , null			as operador_cpf
      , ACC.name        as operador_nome
      , '3199999999'    as operador_fone_fixo
      , null            as operador_fone_celular
      , null            as operador_observacoes
      , ACC.email		as operador_email
      , ACC.created_at	as operador_data_inclusao
      , null			as operador_responsavel_nome
      , null			as operador_responsavel_email
      , null			as operador_responsavel_fone
      , 'FORNECEDOR'    as operador_perfil
from account as ACC
     inner join project_company_account as PCA
                 on PCA.account_id = ACC.id
                and (
                    (%(project_company_account_ids)s::bigint[] is null and PCA.deleted = false)
                    or PCA.id = any(%(project_company_account_ids)s::bigint[])
                )
     inner join project as PRJ
                 on PRJ.id = PCA.project_id
                and PRJ.deleted = false
                and PRJ.company_id = %(company_id)s
                and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
     inner join company as CPN
                 on CPN.id = PRJ.company_id
where ACC.deleted = false
  and not (ACC.active = false and ACC.uid is null)
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF
      where CF.project_id = PRJ.id and CF.company_id = PCA.provider_id
  )) ) as finally
group by contratante_cnpj
      , operador_identificador
      , operador_cpf
      , operador_nome
      , operador_fone_fixo
      , operador_fone_celular
      , operador_observacoes
      , operador_email
      , operador_data_inclusao
      , operador_responsavel_nome
      , operador_responsavel_email
      , operador_responsavel_fone
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta USUARIOS para Parquet.")
    p.add_argument("--company-id", type=int, required=True)
    p.add_argument("--out", required=True, help="Arquivo parquet de saída")

    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--db", dest="dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", default=None, help="Opcional se usar .pgpass")
    p.add_argument("--sslmode", default="require")

    p.add_argument("--chunksize", type=int, default=50_000, help="Linhas por chunk")
    p.add_argument("--compression", default="snappy", help="snappy|gzip|zstd|none")

    return parse_project_args(p, filter_project_company_accounts=True)


def ensure_output_path(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    if path.is_dir():
        raise ValueError(f"--out precisa ser um arquivo: {path}")
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
            params={"company_id": args.company_id, "project_names": args.project_names, "contratada_cnpjs": args.contratada_cnpjs, "project_company_account_ids": args.project_company_account_ids},
            chunksize=args.chunksize,
        ):
            chunk = apply_contrato_mapping(chunk, args)
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            table = adjust_export_datetimes(table)

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
