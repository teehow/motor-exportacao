import ast
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'painel_migracao'))
sys.path.insert(0, str(ROOT / 'motor'))
from core import GROUPS, SCRIPTS, module_from
from migracao_export_dates import DATE_ONLY_COLUMNS, adjust_export_datetimes


class ExportDateTests(unittest.TestCase):
    def test_datetime_shift_midnight_nulls_precision_and_metadata(self):
        for unit in ('s', 'ms', 'us', 'ns'):
            with self.subTest(unit=unit):
                value = datetime(2026, 10, 30, 0, 30, 0)
                table = pa.table({'evento': pa.array([value, None], type=pa.timestamp(unit))}).replace_schema_metadata({b'origem': b'teste'})
                result = adjust_export_datetimes(table)
                self.assertEqual(result['evento'][0].as_py(), value - timedelta(hours=3))
                self.assertIsNone(result['evento'][1].as_py())
                self.assertTrue(result.schema.equals(table.schema, check_metadata=True))
                self.assertEqual(table['evento'][0].as_py(), value)
        nanoseconds = pa.table({'evento': pa.array([pd.Timestamp('2026-10-30 12:34:56.123456789')], type=pa.timestamp('ns'))})
        self.assertEqual(adjust_export_datetimes(nanoseconds)['evento'][0].as_py(), pd.Timestamp('2026-10-30 09:34:56.123456789'))

    def test_midnight_and_timezone_are_not_special_cases(self):
        for tz in (None, 'UTC'):
            value = datetime(2026, 1, 1, tzinfo=timezone.utc if tz else None)
            table = pa.table({'evento': pa.array([value], type=pa.timestamp('us', tz=tz))})
            self.assertEqual(adjust_export_datetimes(table)['evento'][0].as_py(), value - timedelta(hours=3))

    def test_calendar_dates_and_non_timestamp_fields_unchanged(self):
        arrays = {name: pa.array([datetime(2026, 10, 30)], type=pa.timestamp('us')) for name in DATE_ONLY_COLUMNS}
        arrays.update(dia=pa.array([date(2026, 10, 30)], type=pa.date32()),
                      dia64=pa.array([date(2026, 10, 30)], type=pa.date64()),
                      horario=pa.array([123], type=pa.time32('s')),
                      texto=pa.array(['2026-10-30 12:30:00']), numero=pa.array([42]))
        table = pa.table(arrays)
        self.assertTrue(adjust_export_datetimes(table).equals(table, check_metadata=True))

    def test_date_only_list_matches_importer_contract(self):
        tree = ast.parse((ROOT / 'motor' / 'importar_validar.py').read_text(encoding='utf-8-sig'))
        date_columns = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'coerce_date':
                date_columns.update(ast.literal_eval(node.args[1]))
        self.assertEqual(DATE_ONLY_COLUMNS, date_columns)

    def test_empty_all_null_chunked_and_overflow(self):
        for table in (pa.table({'evento': pa.array([], type=pa.timestamp('us'))}),
                      pa.table({'evento': pa.array([None], type=pa.timestamp('us'))})):
            self.assertTrue(adjust_export_datetimes(table).equals(table))
        table = pa.table({'evento': pa.chunked_array([[datetime(2026, 10, 30, 12)], [None]], type=pa.timestamp('us'))})
        self.assertEqual(adjust_export_datetimes(table)['evento'].to_pylist(), [datetime(2026, 10, 30, 9), None])
        minimum = pa.table({'evento': pa.array([-9223372036854775807], type=pa.timestamp('ns'))})
        with self.assertRaisesRegex(ValueError, 'evento'):
            adjust_export_datetimes(minimum)

    def test_all_new_exporters_adjust_once_after_arrow_conversion(self):
        for group in GROUPS:
            with self.subTest(group=group):
                tree = ast.parse((ROOT / 'motor' / SCRIPTS[group]).read_text(encoding='utf-8-sig'))
                shifts = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'adjust_export_datetimes']
                self.assertEqual(len(shifts), 1)
                conversion = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'from_pandas']
                self.assertEqual(len(conversion), 1)
                writes = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'write_table' and isinstance(node.args[0], ast.Name) and node.args[0].id == 'table']
                self.assertEqual(len(writes), 1)
                self.assertLess(conversion[0].lineno, shifts[0].lineno)
                self.assertLess(shifts[0].lineno, writes[0].lineno)

    def test_analysis_and_procedures_dates_stay_aligned_in_parquet(self):
        results = []
        for name in ('exportar_analises.py', 'exportar_procedimentos.py'):
            module = module_from(ROOT / 'motor', name)
            schema = module.build_parquet_schema(pa)
            values = {}
            for field in schema:
                if pa.types.is_timestamp(field.type):
                    values[field.name] = [datetime(2026, 10, 30, 0, 30)]
                elif pa.types.is_date(field.type):
                    values[field.name] = [date(2026, 10, 30)]
                elif pa.types.is_integer(field.type):
                    values[field.name] = [1]
                else:
                    values[field.name] = ['exemplo']
            normalized = module.normalize_batch(pd.DataFrame(values))
            table = adjust_export_datetimes(pa.Table.from_pandas(normalized, schema=schema, preserve_index=False))
            results.append(table['analise_data_analise'][0].as_py())
            self.assertEqual(table['competencia_data'][0].as_py(), date(2026, 10, 30))
            if 'documento_data_validade' in table.column_names:
                self.assertEqual(table['documento_data_validade'][0].as_py(), datetime(2026, 10, 29, 0, 30))
        self.assertEqual(results, [datetime(2026, 10, 29, 21, 30)] * 2)

    def test_analysis_validity_minus_one_day_preserves_nulls_and_time(self):
        module = module_from(ROOT / 'motor', 'exportar_analises.py')
        schema = module.build_parquet_schema(pa)
        validity = [datetime(2026, 1, 1, 12, 34, 56, 123456), None, datetime(2024, 3, 1), datetime(2026, 10, 30)]
        values = {field.name: [None] * len(validity) for field in schema}
        values.update(competencia_data=[date(2026, 10, 30)] * len(validity),
                      documento_data_validade=validity,
                      documento_data_emissao=validity,
                      analise_data_analise=[datetime(2026, 10, 30, 12)] * len(validity))
        frame = pd.DataFrame(values)
        normalized = module.normalize_batch(frame)
        table = adjust_export_datetimes(pa.Table.from_pandas(normalized, schema=schema, preserve_index=False))
        with tempfile.TemporaryDirectory(dir=ROOT) as folder:
            out = Path(folder) / 'analises.parquet'
            pq.write_table(table, out)
            result = pq.read_table(out)
        self.assertEqual(result['documento_data_validade'].to_pylist(), [
            datetime(2025, 12, 31, 12, 34, 56, 123456), None, datetime(2024, 2, 29), datetime(2026, 10, 29)])
        self.assertEqual(result['documento_data_emissao'].to_pylist(), validity)
        self.assertEqual(result['competencia_data'].to_pylist(), [date(2026, 10, 30)] * len(validity))
        self.assertEqual(result['analise_data_analise'].to_pylist(), [datetime(2026, 10, 30, 9)] * len(validity))
        self.assertEqual(frame['documento_data_validade'][0].to_pydatetime(), validity[0])

    def test_actual_exporters_write_shifted_parquet_without_database(self):
        fields = {
            'exportar_operadores.py': 'operador_data_inclusao',
            'exportar_integracoes.py': 'colaborador_data_aprovacao',
            'exportar_blacklist.py': 'blacklist_data_bloqueio',
        }
        for filename, field in fields.items():
            with self.subTest(filename=filename), tempfile.TemporaryDirectory(dir=ROOT) as folder:
                module = module_from(ROOT / 'motor', filename)
                value = datetime(2026, 10, 30, 1, 20, 15, 123456)
                frame = pd.DataFrame({field: [value, None], 'contrato_numero': ['teste', 'teste'],
                                      'turma_data': [pd.Timestamp('2026-10-30')] * 2})
                args = SimpleNamespace(host='teste', port=5432, dbname='teste', user='teste', password='', sslmode='disable',
                                       company_id=1, project_names=None, contratada_cnpjs=None, project_company_account_ids=None,
                                       chunksize=10, compression='snappy', contrato_numero_mapping={})
                out = Path(folder) / 'resultado.parquet'
                with patch.object(module.psycopg2, 'connect', return_value=Mock()), patch.object(module, 'iter_sql_chunks', return_value=iter([frame])):
                    module.export_to_parquet(args, out)
                exported = pq.read_table(out)
                self.assertEqual(exported[field][0].as_py(), value - timedelta(hours=3))
                self.assertIsNone(exported[field][1].as_py())
                self.assertEqual(exported['turma_data'][0].as_py(), datetime(2026, 10, 30))


if __name__ == '__main__':
    unittest.main()
