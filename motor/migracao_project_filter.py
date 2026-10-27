import argparse
import csv
from pathlib import Path
from migracao_contrato_mapping import parse_contrato_args


CONTRATADAS_CTE = r"""
contratadas_filtradas as (
    select PCF.id, PCF.project_id, PCF.company_id
    from project_company as PCF
    inner join company as CF on CF.id = PCF.company_id and CF.deleted = false
    where PCF.deleted = false
      and %(contratada_cnpjs)s::text[] is not null
      and regexp_replace(CF.cnpj, '[^0-9]', '', 'g') = any(%(contratada_cnpjs)s::text[])
)
"""


def load_csv_data(path: Path, column_names: tuple[str, ...]) -> list[str]:
    if path.suffix.lower() != ".csv":
        raise ValueError(f"{path}: deve ser um arquivo .csv.")
    with path.open(encoding="utf-8-sig", newline="") as source:
        sample = source.read(8192)
        source.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        rows = csv.reader(source, dialect)
        names = {}
        column_index = 0
        first_row = True
        for row in rows:
            if not row or all(not value.strip() for value in row):
                continue
            if first_row:
                first_row = False
                headers = [value.strip().lower() for value in row]
                header = next((name for name in column_names if name in headers), None)
                if header is not None:
                    column_index = headers.index(header)
                    continue
            if len(row) <= column_index:
                raise ValueError(f"{path}, linha {rows.line_num}: coluna ausente.")
            value = row[column_index].strip()
            if not value:
                raise ValueError(f"{path}, linha {rows.line_num}: valor vazio.")
            names[value] = None
    if not names:
        raise ValueError(f"{path}: o CSV deve conter pelo menos um valor.")
    return list(names)


def load_project_data(path: Path) -> list[str]:
    return load_csv_data(path, ("project.name", "project_name", "name"))


def load_pendency_data(path: Path) -> list[int]:
    values = load_csv_data(path, ("pendency.pendency_id", "pendency_id", "id"))
    ids = {}
    for value in values:
        if not value.isascii() or not value.isdecimal() or not 0 < int(value) <= 2147483647:
            raise ValueError(f"{path}: pendency.pendency_id invalido: {value!r}.")
        ids[int(value)] = None
    return list(ids)


def load_contratada_data(path: Path) -> list[str]:
    values = load_csv_data(path, ("company.cnpj", "contratada_cnpj", "cnpj"))
    cnpjs = {}
    for value in values:
        cnpj = value.translate(str.maketrans("", "", "./- "))
        if len(cnpj) != 14 or not cnpj.isascii() or not cnpj.isdecimal():
            raise ValueError(f"{path}: CNPJ de contratada invalido: {value!r}.")
        cnpjs[cnpj] = None
    return list(cnpjs)


def load_project_company_account_data(path: Path) -> list[int]:
    values = load_csv_data(path, ("project_company_account.id", "project_company_account_id", "id"))
    ids = {}
    for value in values:
        if not value.isascii() or not value.isdecimal() or not 0 < int(value) <= 9223372036854775807:
            raise ValueError(f"{path}: project_company_account.id invalido: {value!r}.")
        ids[int(value)] = None
    return list(ids)


def parse_project_args(
    parser: argparse.ArgumentParser, *, filter_project_names: bool = True,
    filter_project_company_accounts: bool = False,
) -> argparse.Namespace:
    if filter_project_company_accounts:
        parser.add_argument(
            "--project-company-account-data", type=Path,
            help="CSV de project_company_account.id; aceita cabecalho id ou primeira coluna sem cabecalho. Ignora deleted apenas desses vinculos selecionados.",
        )
    parser.add_argument(
        "--considerar-inactive", action="store_true",
        help=(
            "Inclui tambem pendencias com deleted=true, plan_id=1 e status de inativacao por movimentacao, projeto ou contratada. "
            "Sem o parametro, considera apenas deleted=false."
        ),
    )
    parser.add_argument(
        "--project-data", type=Path,
        help=("CSV com coluna project.name, project_name ou name; sem cabecalho, usa a primeira coluna."
              if filter_project_names else "Ignorado nesta exportacao: colaboradores nao filtra por project.name."),
    )
    parser.add_argument(
        "--pendency-data", type=Path,
        help="CSV com coluna pendency.pendency_id, pendency_id ou id; sem cabecalho, usa a primeira coluna.",
    )
    parser.add_argument(
        "--contratada-data", type=Path,
        help="CSV com coluna company.cnpj, contratada_cnpj ou cnpj; sem cabecalho, usa a primeira coluna.",
    )
    args = parse_contrato_args(parser)
    if filter_project_company_accounts:
        args.project_company_account_ids = None
        if args.project_company_account_data is not None:
            args.project_company_account_data = args.project_company_account_data.expanduser().resolve()
            try:
                args.project_company_account_ids = load_project_company_account_data(args.project_company_account_data)
            except (OSError, UnicodeError, ValueError, csv.Error) as exc:
                parser.error(str(exc))
    args.project_names = None
    if filter_project_names and args.project_data is not None:
        args.project_data = args.project_data.expanduser().resolve()
        try:
            args.project_names = load_project_data(args.project_data)
        except (OSError, UnicodeError, ValueError, csv.Error) as exc:
            parser.error(str(exc))
    args.pendency_ids = None
    if args.pendency_data is not None:
        args.pendency_data = args.pendency_data.expanduser().resolve()
        try:
            args.pendency_ids = load_pendency_data(args.pendency_data)
        except (OSError, UnicodeError, ValueError, csv.Error) as exc:
            parser.error(str(exc))
    args.contratada_cnpjs = None
    if args.contratada_data is not None:
        args.contratada_data = args.contratada_data.expanduser().resolve()
        try:
            args.contratada_cnpjs = load_contratada_data(args.contratada_data)
        except (OSError, UnicodeError, ValueError, csv.Error) as exc:
            parser.error(str(exc))
    return args
