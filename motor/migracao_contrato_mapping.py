import argparse
import csv
import logging
import re
from pathlib import Path


logger = logging.getLogger("troca_contrato_numero")


def load_contrato_mapping(path: Path) -> dict[int, tuple[str, str] | tuple[str, str, str]]:
    if path.suffix.lower() != ".csv":
        raise ValueError("--troca-contrato-numero deve indicar um arquivo .csv.")
    mapping = {}
    with path.open(encoding="utf-8-sig", newline="") as source:
        sample = source.read(8192)
        source.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel()
            dialect.delimiter = ";" if ";" in sample else ","
        first_row = True
        for line_number, row in enumerate(csv.reader(source, dialect), start=1):
            if not row or all(not value.strip() for value in row):
                continue
            if len(row) not in (3, 4):
                raise ValueError(
                    f"{path}, linha {line_number}: informe project_id;contrato_numero;project.name "
                    "e, opcionalmente, a quarta coluna contratada."
                )
            project_id, numero, ordem_servico = (value.strip() for value in row[:3])
            if first_row:
                first_row = False
                if (project_id.lower(), numero.lower(), ordem_servico.lower()) == (
                    "project_id", "contrato_numero", "project.name",
                ):
                    if len(row) == 4 and row[3].strip().lower() != "contratada":
                        raise ValueError(f"{path}: a quarta coluna deve se chamar contratada.")
                    continue
            if not numero or not ordem_servico:
                raise ValueError(f"{path}, linha {line_number}: os contratos nao podem ser vazios.")
            if not project_id.isascii() or not project_id.isdigit() or not 0 < int(project_id) <= 2147483647:
                raise ValueError(f"{path}, linha {line_number}: project_id deve ser um inteiro positivo valido.")
            project_id = int(project_id)
            values = (numero, ordem_servico)
            if len(row) == 4:
                contratada = re.sub(r"[.\-/\s]", "", row[3])
                if len(contratada) != 14 or not contratada.isascii() or not contratada.isdigit():
                    raise ValueError(
                        f"{path}, linha {line_number}: contratada deve conter um CNPJ de 14 digitos."
                    )
                values += (contratada,)
            if project_id in mapping and mapping[project_id] != values:
                raise ValueError(f"{path}: substituicoes conflitantes para project_id {project_id}.")
            mapping[project_id] = values
    if not mapping:
        raise ValueError(f"{path}: o CSV deve conter pelo menos uma substituicao.")
    return mapping


def parse_contrato_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    parser.add_argument(
        "--troca-contrato-numero", type=Path,
        help=(
            "CSV: project_id;contrato_numero;project.name (ordem de servico);"
            "contratada (CNPJ opcional, usado para classificar a exportacao de contratos)."
        ),
    )
    args = parser.parse_args()
    args.contrato_numero_mapping = {}
    if args.troca_contrato_numero is not None:
        args.troca_contrato_numero = args.troca_contrato_numero.expanduser().resolve()
        try:
            args.contrato_numero_mapping = load_contrato_mapping(args.troca_contrato_numero)
        except (OSError, UnicodeError, ValueError, csv.Error) as exc:
            parser.error(str(exc))
        logger.info(
            "Troca de contrato_numero: %s projetos carregados de %s (chave: project_id)",
            len(args.contrato_numero_mapping), args.troca_contrato_numero,
        )
    return args


def apply_contrato_mapping(df, args, *, classify_contratada=False):
    if "contrato_numero" not in df.columns:
        return df
    mapping = getattr(args, "contrato_numero_mapping", {})
    project_column = "_contrato_project_id"
    out = df.drop(columns=[project_column], errors="ignore").copy()
    if mapping:
        if project_column not in df.columns:
            raise ValueError("A consulta nao retornou project.id para aplicar --troca-contrato-numero.")
        matched = df[project_column].isin(mapping)
        out.loc[matched, "contrato_numero"] = df.loc[matched, project_column].map(
            lambda project_id: mapping[project_id][0],
        )
        if "contrato_numero_ordem_servico" in out.columns:
            out.loc[matched, "contrato_numero_ordem_servico"] = df.loc[matched, project_column].map(
                lambda project_id: mapping[project_id][1],
            )
        if classify_contratada:
            contratadas = {project_id: values[2] for project_id, values in mapping.items() if len(values) == 3}
            classified = df[project_column].isin(contratadas)
            if classified.any():
                if not {"contratada_cnpj", "contratada_tipo"}.issubset(out.columns):
                    raise ValueError("A consulta deve retornar contratada_cnpj e contratada_tipo para classificar contratos.")
                actual_cnpj = out.loc[classified, "contratada_cnpj"].astype("string").str.replace(
                    r"[.\-/\s]", "", regex=True,
                )
                expected_cnpj = df.loc[classified, project_column].map(contratadas).astype("string")
                same_cnpj = actual_cnpj.eq(expected_cnpj).fillna(False)
                out.loc[classified, "contratada_tipo"] = same_cnpj.map(
                    {True: "Contratada", False: "Subcontratada"},
                )
                logger.info(
                    "Classificacao pela planilha: %s contratadas | %s subcontratadas",
                    int(same_cnpj.sum()), int((~same_cnpj).sum()),
                )
        logger.info(
            "Troca por project_id: %s linhas | %s correspondencias | %s sem correspondencia, mantidas",
            len(df), int(matched.sum()), int((~matched).sum()),
        )
        for project_id in df.loc[matched, project_column].drop_duplicates().head(5):
            numero, ordem_servico = mapping[project_id][:2]
            logger.info("TROCA CONTRATO | project_id=%s | contrato=%s | ordem_servico=%s", project_id, numero, ordem_servico)
    null_count = int(out["contrato_numero"].isna().sum())
    if null_count:
        raise ValueError(
            f"A origem retornou {null_count} registros com contrato_numero nulo. "
            "O lote nao sera gravado; corrija o valor na origem."
        )
    return out
