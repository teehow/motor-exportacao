from datetime import timedelta

import pyarrow as pa
import pyarrow.compute as pc


# Date-only fields keep their calendar day, even in a legacy timestamp schema.
DATE_ONLY_COLUMNS = frozenset({
    'competencia_data',
    'contrato_data_inicio_vigencia',
    'contrato_data_fim_vigencia',
    'contratada_data_inicio_trabalhista',
    'contratada_data_fim_trabalhista',
    'contratada_data_inicio_mobilizacao',
    'contratada_data_fim_mobilizacao',
    'colaborador_data_nascimento',
    'colaborador_data_inicio_servico',
    'colaborador_data_termino_contrato',
    'colaborador_data_admissao',
    'colaborador_data_desligamento',
    'colaborador_data_demissao',
    'turma_data',
    'documento_data_emissao',
    'documento_data_validade',
})


def adjust_export_datetimes(table: pa.Table) -> pa.Table:
    """Subtract exactly three hours from datetime fields once, before writing."""
    result = table
    for index, field in enumerate(table.schema):
        if field.name in DATE_ONLY_COLUMNS or not pa.types.is_timestamp(field.type):
            continue
        offset = pa.scalar(timedelta(hours=3), type=pa.duration(field.type.unit))
        column = table.column(index)
        try:
            # Pandas NaT may carry int64.min in null slots; do not subtract from it.
            operands = pc.fill_null(column, pa.scalar(0, type=field.type)) if column.null_count else column
            values = pc.subtract_checked(operands, offset)
            if column.null_count:
                values = pc.if_else(pc.is_null(column), pa.scalar(None, type=field.type), values)
        except (pa.ArrowInvalid, OverflowError) as exc:
            raise ValueError(f'Nao foi possivel subtrair 3 horas de {field.name}: {exc}') from exc
        result = result.set_column(index, field, values)
    return result
