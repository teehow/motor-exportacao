import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app import App
from core import defaults


class InterfaceTests(unittest.TestCase):
    def setUp(self):
        self.app = App()
        self.addCleanup(self.app.destroy)

    def test_form_roundtrip_and_small_window(self):
        config = defaults()
        config.update(company_ids='7455,8440', groups=['analises'], cleanup=True)
        self.app.set_config(config)
        self.assertEqual(self.app.collect(), config)
        self.app.geometry('900x650')
        self.app.update()
        self.assertTrue(self.app.progress.winfo_ismapped())
        self.assertTrue(self.app.start_button.winfo_ismapped())
        self.assertTrue(self.app.progress.winfo_rooty() < self.app.winfo_rooty() + self.app.winfo_height())
        self.app.set_running(True)
        self.assertEqual(str(self.app.start_button['state']), 'disabled')
        self.app.set_running(False)

    def test_simulation_from_start_to_report(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as folder:
            config = defaults()
            config.update(simulate=True, company_ids='1', output_dir=folder, groups=['operadores'], mode='Somente exportacao')
            config['source'].update(database='origem_teste', user='leitura')
            self.app.set_config(config)
            self.app.start()
            limit = time.monotonic() + 10
            while self.app.active and time.monotonic() < limit:
                self.app.update()
                time.sleep(0.02)
            self.assertFalse(self.app.active)
            self.assertEqual(len(self.app.rows), 1)
            self.assertEqual(self.app.rows['0']['status'], 'Simulado')
            self.assertTrue(Path(self.app.latest_report).is_file())


if __name__ == '__main__':
    unittest.main()
