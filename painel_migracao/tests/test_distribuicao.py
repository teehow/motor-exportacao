import ast
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT))
import empacotar
import verificar_ambiente


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class DistributionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(dir=PROJECT)
        cls.folder = Path(cls.temp.name)
        cls.archive, cls.count = empacotar.build(PROJECT, cls.folder / 'dist')
        with zipfile.ZipFile(cls.archive) as archive:
            archive.extractall(cls.folder / 'outro computador com espacos')
        cls.root = cls.folder / 'outro computador com espacos' / 'migracao-bex'

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_allowlist_and_manifest(self):
        with zipfile.ZipFile(self.archive) as archive:
            names = {name.removeprefix('migracao-bex/') for name in archive.namelist()}
        self.assertEqual(names, set(empacotar.FILES) | {'MANIFESTO.json'})
        self.assertEqual(len(names), self.count)
        self.assertFalse(any(name.startswith(('.venv/', 'saida/', 'dist/')) or '__pycache__' in name or name.endswith(('.parquet', '.log', '.xlsx', '.bak')) for name in names))
        self.assertEqual(verificar_ambiente.check_integrity(self.root), [])

    def test_portable_defaults_and_simulation(self):
        module = load(self.root / 'painel_migracao' / 'core.py', 'portable_core')
        config = module.defaults()
        self.assertEqual(Path(config['script_dir']), self.root / 'motor')
        self.assertEqual(Path(config['output_dir']), self.root / 'saida')
        self.assertEqual(config['source']['user'], '')
        self.assertEqual(config['source']['database'], '')
        self.assertEqual(config['target']['password'], '')
        self.assertEqual(config['company_ids'], '')
        self.assertTrue(all(not value for value in config['filters'].values()))
        config.update(company_ids='1', groups=['operadores'], simulate=True, mode='Somente exportacao')
        config['source'].update(database='exemplo', user='leitura')
        module.validate(config)
        events = []
        run = module.MigrationRun(config, lambda kind, value: events.append((kind, value)))
        run.execute()
        self.assertEqual(events[-1][1]['status'], 'Simulada')
        self.assertTrue((run.run_dir / 'relatorio.html').is_file())
        self.assertEqual(Path(run.steps[0]['command'][1]).name, 'exportar_operadores.py')

    def test_cli_dry_run_connections_and_redaction(self):
        command = [sys.executable, str(self.root / 'motor' / 'executar_migracao.py'), '--company-id', '1',
                   '--data-dir', str(self.root / 'dados' / 'exemplos'), '--dry-run',
                   '--source-host', 'origem.exemplo', '--source-db', 'origem', '--source-user', 'leitura',
                   '--source-password', 'SEGREDO_TESTE_ORIGEM', '--sqlserver-host', 'destino.exemplo',
                   '--sqlserver-db', 'destino', '--sqlserver-user', 'escrita', '--sqlserver-password', 'SEGREDO_TESTE_DESTINO']
        result = subprocess.run(command, capture_output=True, encoding='utf-8', errors='replace', timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('SEGREDO_TESTE', result.stdout)
        self.assertIn('origem.exemplo', result.stdout)
        self.assertIn('destino.exemplo', result.stdout)
        self.assertIn('exportar_analises.py', result.stdout)
        self.assertNotIn('motor-migracao-', result.stdout)

    def test_integrity_and_all_entrypoints_help(self):
        result = subprocess.run([sys.executable, str(self.root / 'verificar_ambiente.py'), '--integridade'], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for name in empacotar.MOTOR_FILES:
            if name.startswith('migracao_'):
                continue
            with self.subTest(name=name):
                result = subprocess.run([sys.executable, str(self.root / 'motor' / name), '--help'], capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_fixed_passwords_are_rejected(self):
        for source in ('p.add_argument("--password", default="SENHA_FICTICIA")',
                       'p.add_argument("--source-password", default=os.getenv("SENHA", "SENHA_FICTICIA"))'):
            with self.assertRaises(ValueError):
                empacotar.audit_source('teste.py', source.encode())

    def test_sql_and_importer_rules_preserved(self):
        legacy_dir = os.getenv('MOTOR_LEGADO_DIR')
        if not legacy_dir:
            self.skipTest('Defina MOTOR_LEGADO_DIR para comparar com a fonte anterior.')
        legacy = Path(legacy_dir)
        mapping = {
            'motor-migracao-eb-5506-gerar-contratos.py': 'exportar_contratos.py',
            'motor-migracao-eb-5507-gerar-colaboradores.py': 'exportar_colaboradores.py',
            'motor-migracao-eb-5508-gerar-analises.py': 'exportar_analises.py',
            'motor-migracao-eb-5509-gerar-integracoes.py': 'exportar_integracoes.py',
            'motor-migracao-eb-5510-gerar-riscos.py': 'exportar_riscos.py',
            'motor-migracao-eb-5511-gerar-blacklist.py': 'exportar_blacklist.py',
            'motor-migracao-eb-5512-gerar-procedimento.py': 'exportar_procedimentos.py',
            'motor-migracao-eb-5516-gerar-operadores.py': 'exportar_operadores.py',
            'motor-migracao-eb-5516-gerar-operadores_vinculos.py': 'exportar_operadores_vinculos.py',
            'motor-migracao-db-5513-importar-validar.py': 'importar_validar.py',
        }
        if not legacy.is_dir():
            self.skipTest('Fonte legada disponivel somente no computador de desenvolvimento.')
        for old, new in mapping.items():
            with self.subTest(name=new):
                before = ast.parse((legacy / old).read_text(encoding='utf-8-sig'))
                after = ast.parse((self.root / 'motor' / new).read_text(encoding='utf-8-sig'))
                if new == 'importar_validar.py':
                    self.assertEqual(ast.dump(before), ast.dump(after))
                else:
                    def sql_definitions(tree):
                        return {
                            target.id: ast.dump(node.value)
                            for node in tree.body if isinstance(node, ast.Assign)
                            for target in node.targets if isinstance(target, ast.Name)
                            and ('SQL' in target.id or target.id.endswith('_CTES'))
                        }
                    self.assertEqual(sql_definitions(before), sql_definitions(after))


if __name__ == '__main__':
    unittest.main()
