import ast
import copy
import importlib.util
import json
import queue
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import core


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = core.defaults()
        self.config['company_ids'] = '7455'
        self.config['source'].update(database='origem_teste', user='leitura')
        self.config['target'].update(database='destino_teste', user='escrita')
        self.config['output_dir'] = str(self.root)
        self.config['source']['password'] = 'ORIGEM{;SECRETA'
        self.config['target']['password'] = 'DESTINO};SECRETO'

    def test_profiles_and_reports_never_contain_passwords(self):
        config = core.public_config(self.config)
        self.assertNotIn('password', config['source'])
        path = self.root / 'perfil.json'
        path.write_text(json.dumps(self.config), encoding='utf-8')
        self.assertEqual(core.load_profile(path)['target']['password'], '')
        redacted = core.redact('X ' + self.config['target']['password'], self.config)
        self.assertNotIn(self.config['target']['password'], redacted)

    def test_company_ids(self):
        self.config['company_ids'] = '7455, 8440;7455'
        self.assertEqual(core.company_ids(self.config), ['7455', '8440'])
        for value in ('', 'x', '0', '-1', '9999999999999999'):
            self.config['company_ids'] = value
            with self.assertRaises(ValueError):
                core.company_ids(self.config)

    def test_all_generated_commands_parse_and_connections_override(self):
        self.config.update(cnpj='08873873000110')
        self.config['source'].update(host='source.example', port=15432, user='reader')
        self.config['target'].update(host='target.example', port=11433, user='writer')
        steps = core.build_steps(self.config, self.root)
        self.assertEqual(len(steps), 18)
        self.assertEqual([s['group'] for s in steps[:9]], list(core.GROUPS))
        for step in steps:
            command = step['command']
            module = core.module_from(self.config['script_dir'], Path(command[1]).name)
            with patch.object(sys, 'argv', command[1:]):
                args = module.parse_args()
            if step['phase'] == 'Exportacao':
                self.assertEqual(args.host, 'source.example')
                self.assertEqual(args.port, 15432)
            else:
                self.assertEqual(args.sqlserver_host, 'target.example')
                self.assertEqual(args.sqlserver_port, 11433)
                self.assertEqual(args.only, step['group'])
                self.assertFalse(args.truncate)
                self.assertIsNone(args.contratante_cnpj)

    def test_cleanup_only_first_company_and_selected_groups(self):
        self.config.update(company_ids='7455,8440', groups=['operadores'], cleanup=True, cnpj='08873873000110')
        steps = core.build_steps(self.config, self.root)
        imports = [s for s in steps if s['phase'] == 'Importacao']
        self.assertIn('--truncate', imports[0]['command'])
        self.assertIn('--contratante-cnpj', imports[0]['command'])
        self.assertNotIn('--truncate', imports[1]['command'])
        self.assertNotIn('--contratante-cnpj', imports[1]['command'])
        self.assertEqual(len(steps), 4)

    def test_import_only_uses_existing_folder(self):
        self.config.update(mode='Somente importacao', groups=['riscos'], import_dir=str(self.root))
        steps = core.build_steps(self.config, self.root / 'report')
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]['parquet'], str(self.root / 'riscos.parquet'))

    def test_criticas_validation(self):
        self.config.update(criticas=True, cnpj='')
        with self.assertRaisesRegex(ValueError, 'Criticas'):
            core.validate(self.config)

    def test_progress_counters(self):
        self.assertEqual(core.progress('Registros exportados: 1500; lote: 500', 'Exportacao', 1000), 1500)
        self.assertEqual(core.progress('Lote gravado: 50 linhas; total acumulado: 700', 'Exportacao', 0), 700)
        self.assertEqual(core.progress('[OK] migracao_colaborador: 120 linhas inseridas.', 'Importacao', 80), 200)
        self.assertEqual(core.progress('[INFO] sem dados', 'Importacao', 80), 80)
        step = dict(phase='Importacao', records=999)
        core.update_counts(step, 'riscos.parquet -> migracao_risco | lidas=100 | preparadas=80 | inseridas=78 | falhas=2')
        self.assertEqual((step['read_records'], step['prepared_records'], step['records'], step['failed_records']), (100, 80, 78, 2))

    def make_run(self, fail=False, slow=False):
        command = [sys.executable, str(ROOT / 'tests' / 'fixtures' / 'fake_export.py'), '--out', str(self.root / 'fake.parquet')]
        if fail:
            command.append('--fail')
        if slow:
            command.append('--slow')
        step = dict(id='0', company='7455', group='riscos', phase='Exportacao', command=command,
                    parquet=str(self.root / 'fake.parquet'), status='Pendente', records=0,
                    started_at='', ended_at='', seconds=0, log='', message='', partial=True)
        events = []
        with patch.object(core, 'build_steps', return_value=[step]):
            run = core.MigrationRun(self.config, lambda kind, value: events.append((kind, copy.deepcopy(value))))
        return run, events

    def test_real_subprocess_progress_parquet_and_report(self):
        run, events = self.make_run()
        run.execute()
        self.assertEqual(run.steps[0]['status'], 'Concluido')
        self.assertEqual(run.steps[0]['records'], 3)
        self.assertFalse(run.steps[0]['partial'])
        self.assertTrue(any(kind == 'step' and value['records'] > 0 for kind, value in events))
        report = json.loads((run.run_dir / 'execucao.json').read_text(encoding='utf-8'))
        self.assertEqual(report['parquets'][0]['rows'], 3)
        self.assertNotIn(self.config['target']['password'], json.dumps(report))

    def test_failed_subprocess_report(self):
        run, events = self.make_run(fail=True)
        run.execute()
        self.assertEqual(run.steps[0]['status'], 'Erro')
        self.assertEqual(events[-1][1]['status'], 'Erro')
        self.assertTrue((run.run_dir / 'relatorio.html').is_file())

    def test_output_failure_always_finishes_ui_job(self):
        run, events = self.make_run()
        run.run_dir = self.root / 'arquivo'
        run.run_dir.write_text('arquivo de teste', encoding='utf-8')
        run.execute()
        self.assertEqual(events[-1][0], 'done')
        self.assertEqual(events[-1][1]['report'], '')

    def test_cancellation_stops_process_and_reports(self):
        run, events = self.make_run(slow=True)
        thread = threading.Thread(target=run.execute)
        thread.start()
        limit = time.monotonic() + 8
        while run.process is None and time.monotonic() < limit:
            time.sleep(0.05)
        run.cancel()
        thread.join(15)
        self.assertFalse(thread.is_alive())
        self.assertEqual(events[-1][1]['status'], 'Cancelada')
        self.assertTrue(run.process is None or run.process.poll() is not None)

    def test_simulation_never_starts_process(self):
        run, events = self.make_run()
        run.config['simulate'] = True
        with patch.object(core.subprocess, 'Popen') as process:
            run.execute()
        process.assert_not_called()
        self.assertEqual(run.steps[0]['status'], 'Simulado')

    def test_critical_results_and_parameterized_cnpj(self):
        run, _ = self.make_run()
        run.run_dir.mkdir(parents=True)
        run.config['cnpj'] = '08.873.873/0001-10'
        cursor = Mock()
        cursor.description = [('retorno',)]
        cursor.messages = []
        cursor.fetchmany.side_effect = [[('<valor>',)], []]
        cursor.nextset.return_value = False
        connection = Mock()
        connection.cursor.return_value = cursor
        step = run.steps[0]
        with patch.dict(sys.modules, {'pyodbc': SimpleNamespace(connect=Mock(return_value=connection))}):
            run.criticas(step)
        self.assertIn('?', cursor.execute.call_args.args[0])
        self.assertEqual(cursor.execute.call_args.args[1], '08873873000110')
        self.assertEqual(step['records'], 1)
        connection.commit.assert_called_once()
        self.assertIn('&lt;valor&gt;', (run.run_dir / 'criticas.html').read_text())

    def test_importer_selected_files_validation_and_odbc_escaping(self):
        path = Path(self.config['script_dir']) / core.IMPORTER
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        main = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'main')
        self.assertTrue(any(isinstance(node, ast.For) and isinstance(node.iter, ast.Name) and node.iter.id == 'names' for node in ast.walk(main)))
        module = core.module_from(self.config['script_dir'], core.IMPORTER)
        args = SimpleNamespace(sqlserver_driver='ODBC Driver 17 for SQL Server', sqlserver_host='host', sqlserver_port=1433,
                               sqlserver_db='database', sqlserver_user='user', sqlserver_password='p};ss')
        with patch.object(module, 'create_engine', return_value=Mock()) as create:
            module.build_engine(args)
        from urllib.parse import unquote_plus
        self.assertIn('PWD={p}};ss};', unquote_plus(create.call_args.args[0]))


if __name__ == '__main__':
    unittest.main()
