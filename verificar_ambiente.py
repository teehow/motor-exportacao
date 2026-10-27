from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import struct
import sys
from pathlib import Path

DEPENDENCIES = {'pandas': 'pandas', 'pyarrow': 'pyarrow', 'psycopg2': 'psycopg2-binary', 'sqlalchemy': 'SQLAlchemy', 'pyodbc': 'pyodbc', 'tkinter': None}


def check_integrity(root):
    manifest_path = root / 'MANIFESTO.json'
    if not manifest_path.is_file():
        return ['MANIFESTO.json ausente: esta pasta nao e um pacote gerado.']
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    errors = []
    for name, expected in manifest['files'].items():
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()):
            errors.append(f'Caminho invalido no manifesto: {name}')
        elif not path.is_file():
            errors.append(f'Arquivo ausente: {name}')
        elif hashlib.sha256(path.read_bytes()).hexdigest() != expected['sha256']:
            errors.append(f'Arquivo alterado: {name}')
    return errors


def main():
    parser = argparse.ArgumentParser(description='Verifica dependencias locais, sem conectar aos bancos.')
    parser.add_argument('--integridade', action='store_true')
    args = parser.parse_args()
    errors = []
    print(f'Python: {sys.version.split()[0]} | {struct.calcsize("P") * 8} bits')
    if sys.version_info < (3, 12) or struct.calcsize('P') != 8:
        errors.append('Requer Python 3.12 ou superior, 64 bits.')
    for module, distribution in DEPENDENCIES.items():
        try:
            importlib.import_module(module)
            version = importlib.metadata.version(distribution) if distribution else 'incluido no Python'
            print(f'OK {module}: {version}')
        except Exception as exc:
            errors.append(f'{module}: {exc}')
    try:
        import pyodbc
        drivers = [name for name in pyodbc.drivers() if 'SQL Server' in name]
        print('Drivers SQL Server: ' + (', '.join(drivers) if drivers else 'nenhum'))
        if not drivers:
            print('AVISO: instale o Microsoft ODBC Driver for SQL Server para importar ou processar criticas.')
    except ImportError:
        pass
    if args.integridade:
        errors.extend(check_integrity(Path(__file__).resolve().parent))
    for error in errors:
        print('ERRO: ' + error)
    print('Nenhuma conexao com bancos foi realizada.')
    return int(bool(errors))


if __name__ == '__main__':
    raise SystemExit(main())
