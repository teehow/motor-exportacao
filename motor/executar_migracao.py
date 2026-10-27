#!/usr/bin/env python
from __future__ import annotations

import argparse
from migracao_project_filter import parse_project_args
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from migracao_criticas_report import write_criticas_report
from migracao_logging import format_parquet_summary


DEFAULT_SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT_DIR = DEFAULT_SCRIPT_DIR.parent
DEFAULT_OUTPUT_DIR = DEFAULT_ROOT_DIR / "saida"
DEFAULT_DATA_DIR = DEFAULT_ROOT_DIR / "dados"


def build_source_args(password: str | None, connection: dict | None = None) -> list[str]:
    source = dict(host="localhost", port=5432, database="", user="", sslmode="require")
    source.update(connection or {})
    return [
        "--host",
        source["host"],
        "--port",
        str(source["port"]),
        "--sslmode",
        source["sslmode"],
        "--db",
        source["database"],
        "--user",
        source["user"],
        "--password",
        password or "",
    ]


def parse_company_ids(raw: object) -> list[str]:
    values = [raw] if isinstance(raw, str) else raw
    company_ids: list[str] = []
    seen: set[str] = set()

    for value in values:
        for token in re.split(r"[,;\s]+", str(value).strip()):
            if not token:
                continue
            if not token.isdigit():
                raise ValueError(f"company_id invalido: {token!r}")
            if token not in seen:
                seen.add(token)
                company_ids.append(token)

    if not company_ids:
        raise ValueError("Informe ao menos um company_id.")

    return company_ids


def build_commands(
    *,
    script_dir: Path,
    output_dir: Path,
    data_dir: Path,
    company_id: str,
    source_password: str,
    sqlserver_password: str,
    analises_batch_size: int,
    analises_fetch_size: int,
    analises_project_block_size: int,
    analises_parquet_compression: str,
    procedimentos_batch_size: int,
    procedimentos_parquet_compression: str,
    riscos_chunksize: int,
    riscos_project_block_size: int,
    truncate_import: bool,
    contratante_cnpj: str | None,
    project_data: Path | None = None,
    pendency_data: Path | None = None,
    colaboradores_project_block_size: int = 50,
    contratada_data: Path | None = None,
    troca_contrato_numero: Path | None = None,
    considerar_inactive: bool = False,
    project_company_account_data: Path | None = None,
    source_connection: dict | None = None,
    target_connection: dict | None = None,
) -> list[tuple[str, list[str]]]:
    source_args = build_source_args(source_password, source_connection)
    target = dict(host="localhost", port=1433, database="", user="", driver="ODBC Driver 17 for SQL Server")
    target.update(target_connection or {})
    if considerar_inactive:
        source_args.append("--considerar-inactive")
    if pendency_data is not None:
        source_args.extend(["--pendency-data", str(pendency_data)])
    if contratada_data is not None:
        source_args.extend(["--contratada-data", str(contratada_data)])
    if troca_contrato_numero is not None:
        source_args.extend(["--troca-contrato-numero", str(troca_contrato_numero)])
    if project_data is not None:
        source_args.extend(["--project-data", str(project_data)])
    operator_args = []
    if project_company_account_data is not None:
        operator_args.extend(["--project-company-account-data", str(project_company_account_data)])

    return [
        (
            "Gerar operadores",
            [
                sys.executable,
                str(script_dir / "exportar_operadores.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "operadores.parquet"),
                *source_args,
                *operator_args,
            ],
        ),
        (
            "Gerar contratos",
            [
                sys.executable,
                str(script_dir / "exportar_contratos.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "contratos.parquet"),
                "--grupoareas-csv",
                str(data_dir / "grupoAreas.csv"),
                "--grupoareas-encoding",
                "utf-8",
                "--grupoareas-sep",
                ";",
                "--grupoareas-log-limit",
                "1000",
                *source_args,
                "--chunksize",
                "50000",
            ],
        ),
        (
            "Gerar colaboradores",
            [
                sys.executable,
                str(script_dir / "exportar_colaboradores.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "colaboradores.parquet"),
                "--project-block-size",
                str(colaboradores_project_block_size),
                *source_args,
            ],
        ),
        (
            "Gerar analises",
            [
                sys.executable,
                str(script_dir / "exportar_analises.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "analises.parquet"),
                *source_args,
                "--batch-size",
                str(analises_batch_size),
                "--fetch-size",
                str(analises_fetch_size),
                "--project-block-size",
                str(analises_project_block_size),
                "--parquet-compression",
                analises_parquet_compression,
            ],
        ),
        (
            "Gerar procedimentos",
            [
                sys.executable,
                str(script_dir / "exportar_procedimentos.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "procedimentos.parquet"),
                *source_args,
                "--batch-size",
                str(procedimentos_batch_size),
                "--parquet-compression",
                procedimentos_parquet_compression,
            ],
        ),
        (
            "Gerar riscos",
            [
                sys.executable,
                str(script_dir / "exportar_riscos.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "riscos.parquet"),
                "--grupos-csv",
                str(data_dir / "grupos.csv"),
                *source_args,
                "--chunksize",
                str(riscos_chunksize),
                "--project-block-size",
                str(riscos_project_block_size),
            ],
        ),
        (
            "Gerar blacklist",
            [
                sys.executable,
                str(script_dir / "exportar_blacklist.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "blacklist.parquet"),
                *source_args,
            ],
        ),
        (
            "Gerar integracoes",
            [
                sys.executable,
                str(script_dir / "exportar_integracoes.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "integracoes.parquet"),
                *source_args,
            ],
        ),
        (
            "Gerar vinculos de operadores",
            [
                sys.executable,
                str(script_dir / "exportar_operadores_vinculos.py"),
                "--company-id",
                company_id,
                "--out",
                str(output_dir / "operadores_vinculos.parquet"),
                *source_args,
                *operator_args,
            ],
        ),
        (
            "Importar e validar",
            [
                sys.executable,
                str(script_dir / "importar_validar.py"),
                "--contratos",
                str(output_dir / "contratos.parquet"),
                "--colaboradores",
                str(output_dir / "colaboradores.parquet"),
                "--integracoes",
                str(output_dir / "integracoes.parquet"),
                "--operadores",
                str(output_dir / "operadores.parquet"),
                "--operadores-vinculos",
                str(output_dir / "operadores_vinculos.parquet"),
                "--analises",
                str(output_dir / "analises.parquet"),
                "--blacklist",
                str(output_dir / "blacklist.parquet"),
                "--riscos",
                str(output_dir / "riscos.parquet"),
                "--procedimentos",
                str(output_dir / "procedimentos.parquet"),
                "--sqlserver-host",
                target["host"],
                "--sqlserver-port",
                str(target["port"]),
                "--sqlserver-db",
                target["database"],
                "--sqlserver-user",
                target["user"],
                "--sqlserver-password",
                sqlserver_password or "",
                "--sqlserver-driver",
                target["driver"],
                "--company-id",
                company_id,
                *(["--truncate"] if truncate_import else []),
                *(["--contratante-cnpj", contratante_cnpj] if contratante_cnpj else []),
                "--chunksize",
                "10000",
            ],
        ),
    ]


def processar_migracao_critica(password: str, cnpj: str, report_path: Path, connection: dict) -> Path:
    import pyodbc

    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = report_path.with_suffix(".html.tmp")
    def odbc_value(value):
        return "{" + str(value).replace("}", "}}") + "}"

    attributes = dict(DRIVER=connection['driver'], SERVER=f"{connection['host']},{connection['port']}",
                      DATABASE=connection['database'], UID=connection['user'], PWD=password)
    connection = pyodbc.connect(";".join(f"{key}={odbc_value(value)}" for key, value in attributes.items()) + ";TrustServerCertificate=yes;")
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(
            "EXEC processar_migracao_critica @migracao_cnpj=?, @listar_criticas=1",
            cnpj,
        )
        with tmp_path.open("w", encoding="utf-8") as report:
            write_criticas_report(cursor, cnpj, report)
        connection.commit()
        tmp_path.replace(report_path)
        return report_path
    except Exception:
        connection.rollback()
        tmp_path.unlink(missing_ok=True)
        raise
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa a geracao dos parquets e a importacao da migracao Bex.",
    )
    parser.add_argument(
        "--company-id",
        nargs="+",
        default=os.getenv("MIGRACAO_COMPANY_ID"),
        required=not bool(os.getenv("MIGRACAO_COMPANY_ID")),
        help="Um ou mais company_id separados por espaco ou virgula.",
    )
    parser.add_argument(
        "--script-dir",
        type=Path,
        default=Path(os.getenv("MOTOR_MIGRACAO_DIR", DEFAULT_SCRIPT_DIR)),
        help="Pasta dos scripts exportar_*.py e importar_validar.py.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(os.getenv("MIGRACAO_OUTPUT_DIR", DEFAULT_OUTPUT_DIR)),
        help="Pasta de saida dos arquivos parquet.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("MIGRACAO_DATA_DIR", DEFAULT_DATA_DIR)),
        help="Pasta dos CSVs grupos.csv e grupoAreas.csv.",
    )
    parser.add_argument(
        "--source-password",
        default=os.getenv("BEX_SOURCE_PASSWORD"),
        help="Senha do banco de origem. Tambem pode ser informada via BEX_SOURCE_PASSWORD.",
    )
    parser.add_argument("--source-host", default=os.getenv("BEX_SOURCE_HOST", "localhost"))
    parser.add_argument("--source-port", type=int, default=int(os.getenv("BEX_SOURCE_PORT", "5432")))
    parser.add_argument("--source-db", default=os.getenv("BEX_SOURCE_DB", ""))
    parser.add_argument("--source-user", default=os.getenv("BEX_SOURCE_USER", ""))
    parser.add_argument("--source-sslmode", choices=["disable", "allow", "prefer", "require", "verify-ca", "verify-full"], default=os.getenv("BEX_SOURCE_SSLMODE", "require"))
    parser.add_argument("--sqlserver-host", default=os.getenv("GRT_MIGRACAO_HOST", "localhost"))
    parser.add_argument("--sqlserver-port", type=int, default=int(os.getenv("GRT_MIGRACAO_PORT", "1433")))
    parser.add_argument("--sqlserver-db", default=os.getenv("GRT_MIGRACAO_DB", ""))
    parser.add_argument("--sqlserver-user", default=os.getenv("GRT_MIGRACAO_USER", ""))
    parser.add_argument("--sqlserver-driver", default=os.getenv("GRT_MIGRACAO_DRIVER", "ODBC Driver 17 for SQL Server"))
    parser.add_argument(
        "--sqlserver-password",
        default=os.getenv("GRT_MIGRACAO_PASSWORD"),
        help="Senha do SQL Server. Tambem pode ser informada via GRT_MIGRACAO_PASSWORD.",
    )
    parser.add_argument(
        "--contratante-cnpj",
        help="Limita a limpeza ao contratante_cnpj e usa esse CNPJ no processamento final das criticas.",
    )
    parser.add_argument(
        "--processar-criticas",
        action="store_true",
        help="Chama processar_migracao_critica ao final quando --contratante-cnpj tiver valor.",
    )
    parser.add_argument(
        "--analises-batch-size",
        type=int,
        default=int(os.getenv("MIGRACAO_ANALISES_BATCH_SIZE", "50000")),
        help="Tamanho do lote usado na geração streaming de analises.parquet.",
    )
    parser.add_argument(
        "--analises-fetch-size",
        type=int,
        default=int(os.getenv("MIGRACAO_ANALISES_FETCH_SIZE", "5000")),
        help="Quantidade de linhas por busca no PostgreSQL; a escrita usa --analises-batch-size.",
    )
    parser.add_argument(
        "--analises-project-block-size",
        type=int,
        default=int(os.getenv("MIGRACAO_ANALISES_PROJECT_BLOCK_SIZE", "50")),
        help="Quantidade de projetos por bloco na geracao de analises.parquet. Use 0 para desativar.",
    )
    parser.add_argument(
        "--analises-parquet-compression",
        default=os.getenv("MIGRACAO_ANALISES_PARQUET_COMPRESSION", "snappy"),
        help="Compressao do analises.parquet: snappy, gzip, zstd ou none.",
    )
    parser.add_argument(
        "--procedimentos-batch-size",
        type=int,
        default=int(os.getenv("MIGRACAO_PROCEDIMENTOS_BATCH_SIZE", os.getenv("MIGRACAO_ANALISES_BATCH_SIZE", "50000"))),
        help="Tamanho do lote usado na geracao streaming de procedimentos.parquet.",
    )
    parser.add_argument(
        "--procedimentos-parquet-compression",
        default=os.getenv("MIGRACAO_PROCEDIMENTOS_PARQUET_COMPRESSION", os.getenv("MIGRACAO_ANALISES_PARQUET_COMPRESSION", "snappy")),
        help="Compressao do procedimentos.parquet: snappy, gzip, zstd ou none.",
    )
    parser.add_argument(
        "--riscos-chunksize",
        type=int,
        default=int(os.getenv("MIGRACAO_RISCOS_CHUNKSIZE", "50000")),
        help="Quantidade de linhas buscadas/escritas por vez em riscos.parquet.",
    )
    parser.add_argument(
        "--riscos-project-block-size",
        type=int,
        default=int(os.getenv("MIGRACAO_RISCOS_PROJECT_BLOCK_SIZE", "50")),
        help="Quantidade de projetos por bloco na geracao de riscos.parquet. Use 0 para desativar.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra os comandos que seriam executados, sem executar.",
    )
    truncate_group = parser.add_mutually_exclusive_group()
    truncate_group.add_argument(
        "--truncate",
        dest="truncate_import",
        action="store_true",
        help="Limpa os registros existentes no SQL Server antes de importar. Em multiplas empresas, limpa apenas na primeira.",
    )
    truncate_group.add_argument(
        "--no-truncate",
        dest="truncate_import",
        action="store_false",
        help="Mantem os registros existentes no SQL Server antes de importar.",
    )
    parser.set_defaults(truncate_import=False)
    parser.add_argument(
        "--colaboradores-project-block-size", type=int,
        default=int(os.getenv("MIGRACAO_COLABORADORES_PROJECT_BLOCK_SIZE", "50")),
        help="Projetos por bloco na geracao de colaboradores. Use 0 para desativar.",
    )
    return parse_project_args(parser, filter_project_company_accounts=True)


def validate_inputs(script_dir: Path, data_dir: Path) -> None:
    required_scripts = [
        "migracao_contrato_mapping.py",
        "migracao_logging.py",
        "migracao_criticas_report.py",
        "migracao_sql_export.py",
        "migracao_project_filter.py",
        "exportar_operadores.py",
        "exportar_contratos.py",
        "exportar_colaboradores.py",
        "exportar_analises.py",
        "exportar_procedimentos.py",
        "exportar_riscos.py",
        "exportar_blacklist.py",
        "exportar_integracoes.py",
        "exportar_operadores_vinculos.py",
        "importar_validar.py",
    ]
    missing_scripts = [name for name in required_scripts if not (script_dir / name).is_file()]
    missing_csvs = [name for name in ["grupoAreas.csv", "grupos.csv"] if not (data_dir / name).is_file()]

    if missing_scripts:
        raise FileNotFoundError(
            "Scripts nao encontrados em "
            f"{script_dir}: {', '.join(missing_scripts)}"
        )
    if missing_csvs:
        raise FileNotFoundError(
            "Arquivos CSV nao encontrados em "
            f"{data_dir}: {', '.join(missing_csvs)}"
        )


def main() -> int:
    args = parse_args()
    source_connection = dict(host=args.source_host, port=args.source_port, database=args.source_db, user=args.source_user, sslmode=args.source_sslmode)
    target_connection = dict(host=args.sqlserver_host, port=args.sqlserver_port, database=args.sqlserver_db, user=args.sqlserver_user, driver=args.sqlserver_driver)
    if not args.source_db or not args.source_user or not args.sqlserver_db or not args.sqlserver_user:
        raise ValueError("Informe bancos e usuarios de origem/destino pelos parametros ou variaveis de ambiente.")
    if not args.dry_run and not args.sqlserver_password:
        raise ValueError("Informe a senha do SQL Server por GRT_MIGRACAO_PASSWORD ou --sqlserver-password.")
    executor_started = time.monotonic()
    print(f"INICIO | {datetime.now():%d/%m/%Y %H:%M:%S} | executar_migracao.py", flush=True)
    args.contratante_cnpj = (args.contratante_cnpj or "").strip() or None
    company_ids = parse_company_ids(args.company_id)
    script_dir = args.script_dir.resolve()
    output_dir = args.out_dir.resolve()
    data_dir = args.data_dir.resolve()

    validate_inputs(script_dir, data_dir)
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    steps_per_company = 10
    processar_criticas = args.processar_criticas and args.contratante_cnpj is not None
    total_steps = len(company_ids) * steps_per_company + int(processar_criticas)
    global_step = 0

    for company_index, company_id in enumerate(company_ids, start=1):
        company_output_dir = output_dir if len(company_ids) == 1 else output_dir / f"company_{company_id}"
        if not args.dry_run:
            company_output_dir.mkdir(parents=True, exist_ok=True)
        truncate_import = args.truncate_import and company_index == 1

        commands = build_commands(
            script_dir=script_dir,
            output_dir=company_output_dir,
            data_dir=data_dir,
            company_id=company_id,
            source_password=args.source_password,
            sqlserver_password=args.sqlserver_password,
            analises_batch_size=args.analises_batch_size,
            analises_fetch_size=args.analises_fetch_size,
            analises_project_block_size=args.analises_project_block_size,
            analises_parquet_compression=args.analises_parquet_compression,
            procedimentos_batch_size=args.procedimentos_batch_size,
            procedimentos_parquet_compression=args.procedimentos_parquet_compression,
            riscos_chunksize=args.riscos_chunksize,
            riscos_project_block_size=args.riscos_project_block_size,
            truncate_import=truncate_import,
            contratante_cnpj=args.contratante_cnpj,
            project_data=args.project_data,
            pendency_data=args.pendency_data,
            colaboradores_project_block_size=args.colaboradores_project_block_size,
            contratada_data=args.contratada_data,
            troca_contrato_numero=args.troca_contrato_numero,
            considerar_inactive=args.considerar_inactive,
            project_company_account_data=args.project_company_account_data,
            source_connection=source_connection,
            target_connection=target_connection,
        )

        for label, command in commands:
            global_step += 1
            print(f"INICIO | {datetime.now():%d/%m/%Y %H:%M:%S} | [{global_step}/{total_steps}] {label} | company_id={company_id}", flush=True)
            if args.dry_run:
                safe_command = command.copy()
                for password_flag in ("--password", "--sqlserver-password"):
                    if password_flag in safe_command:
                        safe_command[safe_command.index(password_flag) + 1] = "[senha omitida]"
                print(shlex.join(safe_command))
                print(f"FIM | {label} | SIMULACAO", flush=True)
                continue

            log_dir = company_output_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            log_path = log_dir / f"{Path(command[1]).stem}_{stamp}.log"
            env = os.environ.copy()
            env["MIGRACAO_LOG_FILE"] = str(log_path)
            env["PYTHONIOENCODING"] = "utf-8"
            started = time.monotonic()
            with log_path.open("a", encoding="utf-8") as detail:
                result = subprocess.run(command, cwd=script_dir, env=env, stdout=detail, stderr=subprocess.STDOUT)
            if result.returncode != 0:
                print(f"FIM | {label} | ERRO (codigo {result.returncode}) | Log: {log_path}", file=sys.stderr, flush=True)
                print("FIM | executar_migracao.py | ERRO", file=sys.stderr, flush=True)
                return result.returncode
            parquet_summary = format_parquet_summary(command[2:])
            print(f"FIM | {label} | OK | {time.monotonic() - started:.1f}s{parquet_summary}", flush=True)

    if processar_criticas:
        started = time.monotonic()
        print(f"INICIO | {datetime.now():%d/%m/%Y %H:%M:%S} | [{total_steps}/{total_steps}] Processar criticas da migracao", flush=True)
        if not args.dry_run:
            try:
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                safe_cnpj = re.sub(r"[^0-9]", "", args.contratante_cnpj) or "contratante"
                report_path = output_dir / "relatorios" / f"criticas_{safe_cnpj}_{stamp}.html"
                processar_migracao_critica(args.sqlserver_password, args.contratante_cnpj, report_path, target_connection)
                print(f"FIM | Processar criticas | OK | {time.monotonic() - started:.1f}s | Relatorio: {report_path}", flush=True)
            except Exception as exc:
                print(f"Falha ao processar criticas da migracao: {exc}", file=sys.stderr)
                print("FIM | executar_migracao.py | ERRO", file=sys.stderr, flush=True)
                return 1
        else:
            print("FIM | Processar criticas | SIMULACAO", flush=True)

    if not args.dry_run:
        print(f"Logs detalhados: {output_dir} (pasta logs de cada empresa)")
    print(f"FIM | executar_migracao.py | OK | {time.monotonic() - executor_started:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("FIM | executar_migracao.py | INTERROMPIDO", file=sys.stderr, flush=True)
        raise SystemExit(130)
    except Exception as exc:
        print(f"FIM | executar_migracao.py | ERRO: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1)
