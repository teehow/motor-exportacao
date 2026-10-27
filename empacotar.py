from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

VERSION = '1.0.0'
MOTOR_FILES = (
    'executar_migracao.py', 'exportar_operadores.py', 'exportar_contratos.py',
    'exportar_colaboradores.py', 'exportar_analises.py', 'exportar_procedimentos.py',
    'exportar_riscos.py', 'exportar_blacklist.py', 'exportar_integracoes.py',
    'exportar_operadores_vinculos.py', 'importar_validar.py',
    'migracao_contrato_mapping.py', 'migracao_criticas_report.py', 'migracao_html_text.py',
    'migracao_logging.py', 'migracao_project_filter.py', 'migracao_sql_export.py',
)
FILES = (
    'Abrir Painel Migracao.cmd', 'Iniciar Painel Migracao.ps1', 'Instalar.cmd',
    'Executar Migracao.cmd', 'Verificar Ambiente.cmd', 'verificar_ambiente.py',
    'requirements.txt', 'LEIA-ME.md', 'empacotar.py',
    'painel_migracao/app.py', 'painel_migracao/core.py', 'painel_migracao/README.md',
    'dados/LEIA-ME.md', 'dados/exemplos/projetos.csv', 'dados/exemplos/pendencias.csv',
    'dados/exemplos/contratadas.csv', 'dados/exemplos/project_company_account.csv',
    'dados/exemplos/contratoNumero.csv', 'dados/exemplos/grupoAreas.csv', 'dados/exemplos/grupos.csv',
) + tuple('motor/' + name for name in MOTOR_FILES)


def audit_source(name, content):
    text = content.decode('utf-8-sig')
    if str(Path.home()).casefold() in text.casefold():
        raise ValueError(f'Caminho pessoal encontrado em {name}.')
    if name.endswith('.py'):
        tree = ast.parse(text, filename=name)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or node.func.attr != 'add_argument':
                continue
            if not any(isinstance(arg, ast.Constant) and isinstance(arg.value, str) and 'password' in arg.value for arg in node.args):
                continue
            default = next((keyword.value for keyword in node.keywords if keyword.arg == 'default'), None)
            if isinstance(default, ast.Constant) and default.value:
                raise ValueError(f'Senha fixa em {name}.')
            if isinstance(default, ast.Call) and len(default.args) > 1:
                fallback = default.args[1]
                if isinstance(fallback, ast.Constant) and fallback.value:
                    raise ValueError(f'Senha fixa no fallback de ambiente em {name}.')


def build(root, output):
    root = Path(root).resolve()
    output = Path(output).resolve()
    contents = {}
    for name in FILES:
        path = root / name
        if not path.is_file():
            raise FileNotFoundError(f'Arquivo obrigatorio ausente: {name}')
        content = path.read_bytes()
        audit_source(name, content)
        # CMD files use CRLF for native Windows execution.
        if name.endswith('.cmd'):
            content = content.replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
        contents[name] = content
    manifest = dict(version=VERSION, platform='Windows x64', python='3.12+',
                    created_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
                    files={name: dict(bytes=len(content), sha256=hashlib.sha256(content).hexdigest()) for name, content in contents.items()})
    output.mkdir(parents=True, exist_ok=True)
    zip_path = output / f'migracao-bex-windows-{VERSION}.zip'
    descriptor, temporary_name = tempfile.mkstemp(prefix='pacote_', suffix='.zip.tmp', dir=output)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with zipfile.ZipFile(temporary, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for name, content in contents.items():
                archive.writestr('migracao-bex/' + name, content)
            archive.writestr('migracao-bex/MANIFESTO.json', json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8'))
        with zipfile.ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise ValueError('Arquivo ZIP corrompido.')
        temporary.replace(zip_path)
    finally:
        temporary.unlink(missing_ok=True)
    digest = hashlib.sha256(zip_path.read_bytes()).hexdigest()
    zip_path.with_suffix('.zip.sha256').write_text(f'{digest}  {zip_path.name}\n', encoding='ascii')
    return zip_path, len(contents) + 1


def main():
    parser = argparse.ArgumentParser(description='Gera distribuicao limpa do painel e do motor.')
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--out-dir', type=Path)
    args = parser.parse_args()
    path, count = build(args.root, args.out_dir or args.root / 'dist')
    print(f'Pacote: {path}\nArquivos: {count}\nTamanho: {path.stat().st_size:,} bytes')


if __name__ == '__main__':
    main()
