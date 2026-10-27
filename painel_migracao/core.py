from __future__ import annotations

import copy
import html
import importlib.util
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, quote_plus

GROUPS = ('operadores', 'contratos', 'colaboradores', 'analises', 'procedimentos', 'riscos', 'blacklist', 'integracoes', 'operadores_vinculos')
SCRIPTS = dict(zip(GROUPS, (
    'exportar_operadores.py', 'exportar_contratos.py',
    'exportar_colaboradores.py', 'exportar_analises.py',
    'exportar_procedimentos.py', 'exportar_riscos.py',
    'exportar_blacklist.py', 'exportar_integracoes.py',
    'exportar_operadores_vinculos.py')))
IMPORTER = 'importar_validar.py'
FILTERS = {
    'project_data': '--project-data', 'pendency_data': '--pendency-data',
    'contratada_data': '--contratada-data', 'project_company_account_data': '--project-company-account-data',
    'troca_contrato_numero': '--troca-contrato-numero',
}


def defaults():
    root = Path(__file__).resolve().parent.parent
    data = root / 'dados'
    python = root / '.venv' / 'Scripts' / 'python.exe'
    return dict(
        python=str(python if python.is_file() else Path(sys.executable)),
        script_dir=str(Path(os.getenv('MOTOR_MIGRACAO_DIR', root / 'motor'))),
        output_dir=str(root / 'saida'), import_dir=str(root / 'saida'),
        company_ids=os.getenv('MIGRACAO_COMPANY_ID', ''), cnpj='', mode='Completa', groups=list(GROUPS),
        cleanup=False, criticas=False, considerar_inactive=False, simulate=False, row_fallback=False,
        source=dict(host='localhost', port=5432, database='', user='', password='', sslmode='require'),
        target=dict(host='localhost', port=1433, database='', user='', password='', driver='ODBC Driver 17 for SQL Server'),
        filters={key: '' for key in FILTERS},
        tuning=dict(analises_batch_size=50000, analises_fetch_size=5000, analises_project_block_size=50,
                    analises_parquet_compression='snappy', procedimentos_batch_size=50000,
                    procedimentos_parquet_compression='snappy', riscos_chunksize=50000,
                    riscos_project_block_size=50, colaboradores_project_block_size=50,
                    export_chunksize=50000, export_compression='snappy', import_chunksize=1000),
        grupos_csv=str(data / 'grupos.csv'), grupoareas_csv=str(data / 'grupoAreas.csv'),
    )


def public_config(config):
    result = copy.deepcopy(config)
    for section in ('source', 'target'):
        result[section].pop('password', None)
    return result


def load_profile(path):
    current = defaults()
    incoming = json.loads(Path(path).read_text(encoding='utf-8'))
    for key in current:
        if key in incoming:
            if isinstance(current[key], dict):
                current[key].update({k: v for k, v in incoming[key].items() if k in current[key] and k != 'password'})
            else:
                current[key] = incoming[key]
    return current


def company_ids(config):
    values = re.split(r'[,;\s]+', str(config['company_ids']).strip())
    if not values or any(not value.isascii() or not value.isdecimal() or not 0 < int(value) <= 2147483647 for value in values):
        raise ValueError('Informe company_id(s) inteiros positivos separados por virgula ou espaco.')
    return list(dict.fromkeys(str(int(value)) for value in values))


def module_from(script_dir, name):
    script_dir = str(Path(script_dir).resolve())
    if script_dir in sys.path:
        sys.path.remove(script_dir)
    sys.path.insert(0, script_dir)
    for key, existing in list(sys.modules.items()):
        filename = getattr(existing, '__file__', None)
        if key.startswith('migracao_') and filename and Path(filename).parent != Path(script_dir):
            del sys.modules[key]
    path = Path(script_dir) / name
    spec = importlib.util.spec_from_file_location('painel_' + path.stem.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(spec.name, None)
        raise
    return module


def validate(config):
    company_ids(config)
    if config['mode'] not in ('Completa', 'Somente exportacao', 'Somente importacao'):
        raise ValueError('Modo de execucao invalido.')
    if not config['groups'] or any(group not in GROUPS for group in config['groups']):
        raise ValueError('Selecione pelo menos um grupo de dados valido.')
    if not str(config['output_dir']).strip():
        raise ValueError('Informe a pasta de saida e relatorios.')
    scripts = Path(config['script_dir'])
    for path in (Path(config['python']), scripts / 'executar_migracao.py', scripts / 'migracao_project_filter.py'):
        if not path.is_file():
            raise ValueError(f'Arquivo nao encontrado: {path}')
    for group in config['groups']:
        if not (scripts / SCRIPTS[group]).is_file():
            raise ValueError(f'Exportador nao encontrado: {SCRIPTS[group]}')
    importing = config['mode'] != 'Somente exportacao'
    exporting = config['mode'] != 'Somente importacao'
    if importing and not (scripts / IMPORTER).is_file():
        raise ValueError('Importador nao encontrado.')
    for section, needed in (('source', exporting), ('target', importing or config['criticas'])):
        if not needed:
            continue
        connection = config[section]
        for field in ('host', 'database', 'user'):
            if not str(connection[field]).strip():
                raise ValueError(f'Conexao {section}: preencha {field}.')
        if not 1 <= int(connection['port']) <= 65535:
            raise ValueError(f'Porta invalida em {section}.')
    if importing and not config['simulate'] and not config['target']['password']:
        raise ValueError('Informe a senha do SQL Server.')
    cnpj = re.sub(r'\D', '', config['cnpj'])
    if config['cnpj'] and (len(cnpj) != 14 or not cnpj.isascii()):
        raise ValueError('CNPJ deve conter 14 digitos.')
    if config['criticas'] and (not importing or not cnpj):
        raise ValueError('Criticas exigem importacao e CNPJ do contratante.')
    for key, value in config['tuning'].items():
        if key.endswith('compression'):
            if value not in ('snappy', 'gzip', 'zstd', 'none'):
                raise ValueError(f'Compressao invalida: {key}')
        elif int(value) < (0 if key.endswith('project_block_size') else 1):
            raise ValueError(f'Valor invalido: {key}')
    filters = module_from(scripts, 'migracao_project_filter.py')
    loaders = dict(project_data=filters.load_project_data, pendency_data=filters.load_pendency_data,
                   contratada_data=filters.load_contratada_data, project_company_account_data=filters.load_project_company_account_data)
    for key, raw in config['filters'].items():
        if not raw or not exporting:
            continue
        path = Path(raw)
        if key in loaders:
            loaders[key](path)
        elif key == 'troca_contrato_numero':
            module_from(scripts, 'migracao_contrato_mapping.py').load_contrato_mapping(path)
    if exporting:
        if 'riscos' in config['groups'] and not Path(config['grupos_csv']).is_file():
            raise ValueError('Selecione grupos.csv.')
        if 'contratos' in config['groups'] and not Path(config['grupoareas_csv']).is_file():
            raise ValueError('Selecione grupoAreas.csv.')
    if config['mode'] == 'Somente importacao':
        for company in company_ids(config):
            folder = import_folder(config, company)
            for group in config['groups']:
                if not (folder / f'{group}.parquet').is_file():
                    raise ValueError(f'Parquet nao encontrado: {folder / (group + ".parquet")}')


def import_folder(config, company):
    root = Path(config['import_dir']).expanduser().resolve()
    return root / f'company_{company}' if len(company_ids(config)) > 1 else root


def set_arg(command, flag, value):
    if flag in command:
        index = command.index(flag)
        command[index + 1] = str(value)
    else:
        command.extend([flag, str(value)])


def remove_arg(command, flag):
    if flag in command:
        index = command.index(flag)
        del command[index:index + 2]


def build_steps(config, run_dir):
    executor = module_from(config['script_dir'], 'executar_migracao.py')
    steps = []
    tuning = config['tuning']
    source, target = config['source'], config['target']
    for company_index, company in enumerate(company_ids(config)):
        folder = import_folder(config, company) if config['mode'] == 'Somente importacao' else Path(run_dir) / f'company_{company}'
        options = dict(script_dir=Path(config['script_dir']), output_dir=folder, data_dir=Path(config['grupos_csv']).parent,
                       company_id=company, source_password=source['password'], sqlserver_password=target['password'],
                       truncate_import=False, contratante_cnpj=None,
                       considerar_inactive=config['considerar_inactive'],
                       source_connection=source, target_connection=target)
        for key in ('analises_batch_size', 'analises_fetch_size', 'analises_project_block_size',
                    'analises_parquet_compression', 'procedimentos_batch_size', 'procedimentos_parquet_compression',
                    'riscos_chunksize', 'riscos_project_block_size', 'colaboradores_project_block_size'):
            options[key] = tuning[key]
        for key, value in config['filters'].items():
            options[key] = Path(value) if value else None
        commands = executor.build_commands(**options)
        by_script = {Path(command[1]).name: command for _, command in commands}
        if config['mode'] != 'Somente importacao':
            for group in GROUPS:
                if group not in config['groups']:
                    continue
                command = by_script[SCRIPTS[group]].copy()
                command[0] = config['python']
                for flag, field in (('--host', 'host'), ('--port', 'port'), ('--db', 'database'), ('--user', 'user'), ('--password', 'password'), ('--sslmode', 'sslmode')):
                    set_arg(command, flag, source[field])
                if '--grupos-csv' in command:
                    set_arg(command, '--grupos-csv', config['grupos_csv'])
                if '--grupoareas-csv' in command:
                    set_arg(command, '--grupoareas-csv', config['grupoareas_csv'])
                if group not in ('analises', 'procedimentos', 'riscos'):
                    set_arg(command, '--chunksize', tuning['export_chunksize'])
                if group in ('operadores', 'operadores_vinculos', 'colaboradores', 'integracoes'):
                    set_arg(command, '--compression', tuning['export_compression'])
                steps.append(dict(company=company, group=group, phase='Exportacao', command=command, parquet=str(folder / f'{group}.parquet')))
        if config['mode'] != 'Somente exportacao':
            for group in GROUPS:
                if group not in config['groups']:
                    continue
                command = by_script[IMPORTER].copy()
                command[0] = config['python']
                for flag, field in (('--sqlserver-host', 'host'), ('--sqlserver-port', 'port'), ('--sqlserver-db', 'database'), ('--sqlserver-user', 'user'), ('--sqlserver-password', 'password'), ('--sqlserver-driver', 'driver')):
                    set_arg(command, flag, target[field])
                set_arg(command, '--only', group)
                set_arg(command, '--chunksize', tuning['import_chunksize'])
                set_arg(command, '--error-dir', Path(run_dir) / 'erros' / company / group)
                if config['row_fallback']:
                    command.append('--row-fallback')
                if config['cleanup'] and company_index == 0:
                    command.append('--truncate')
                    if config['cnpj']:
                        set_arg(command, '--contratante-cnpj', re.sub(r'\D', '', config['cnpj']))
                steps.append(dict(company=company, group=group, phase='Importacao', command=command, parquet=str(folder / f'{group}.parquet')))
    if config['criticas']:
        steps.append(dict(company='', group='criticas', phase='Criticas', command=[], parquet=''))
    for index, step in enumerate(steps):
        step.update(id=str(index), status='Pendente', records=0, read_records=0, prepared_records=0, failed_records=0,
                    started_at='', ended_at='', seconds=0, log='', message='', partial=True)
    return steps


def redact(text, config):
    for section in ('source', 'target'):
        secret = config[section].get('password', '')
        if secret:
            for value in sorted({secret, quote(secret, safe=''), quote_plus(secret), secret.replace('}', '}}')}, key=len, reverse=True):
                text = text.replace(value, '[senha omitida]')
    return text


def odbc_string(target):
    def escape(value):
        return '{' + str(value).replace('}', '}}') + '}'
    return ';'.join(f'{key}={escape(value)}' for key, value in (
        ('DRIVER', target['driver']), ('SERVER', f"{target['host']},{target['port']}"),
        ('DATABASE', target['database']), ('UID', target['user']), ('PWD', target['password']),
        ('TrustServerCertificate', 'yes'))) + ';'


def test_connection(section, connection):
    if section == 'source':
        import psycopg2
        conn = psycopg2.connect(host=connection['host'], port=int(connection['port']), dbname=connection['database'],
                                user=connection['user'], password=connection['password'] or None, sslmode=connection['sslmode'], connect_timeout=8)
        try:
            conn.set_session(readonly=True)
            with conn.cursor() as cursor:
                cursor.execute('SELECT current_database()')
                return str(cursor.fetchone()[0])
        finally:
            conn.close()
    import pyodbc
    conn = pyodbc.connect(odbc_string(connection), timeout=8)
    try:
        return str(conn.cursor().execute('SELECT DB_NAME()').fetchone()[0])
    finally:
        conn.close()


def parquet_count(path):
    import pyarrow.parquet as pq
    with pq.ParquetFile(path) as parquet:
        return parquet.metadata.num_rows


def progress(line, phase, current):
    if phase == 'Importacao':
        match = re.search(r'\[OK\].*?:\s*(\d+) linhas inseridas', line)
        return current + int(match[1]) if match else current
    for pattern in (r'total acumulado:\s*([\d.,]+)', r'Registros exportados(?: acumulados)?:\s*([\d.,]+)',
                    r'Total de registros(?: exportados)?:\s*([\d.,]+)', r'Linhas retornadas:\s*([\d.,]+)'):
        match = re.search(pattern, line, re.I)
        if match:
            return max(current, int(re.sub(r'\D', '', match[1])))
    return current


def update_counts(step, line):
    step['records'] = progress(line, step['phase'], step['records'])
    if step['phase'] == 'Importacao':
        match = re.search(r'lidas=(\d+)\s*\|\s*preparadas=(\d+)\s*\|\s*inseridas=(\d+)\s*\|\s*falhas=(\d+)', line)
        if match:
            step.update(read_records=int(match[1]), prepared_records=int(match[2]),
                        records=int(match[3]), failed_records=int(match[4]))


class MigrationRun:
    def __init__(self, config, emit):
        self.config = copy.deepcopy(config)
        self.emit = emit
        self.cancelled = threading.Event()
        self.lock = threading.Lock()
        self.process = None
        self.critical_cursor = None
        self.run_dir = Path(config['output_dir']).expanduser().resolve() / 'execucoes' / datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        self.steps = build_steps(config, self.run_dir)

    def cancel(self):
        self.cancelled.set()
        with self.lock:
            if self.process and self.process.poll() is None:
                self.process.terminate()
            cursor = self.critical_cursor
        if cursor is not None:
            try:
                cursor.cancel()
            except Exception:
                pass

    def update(self, step):
        self.emit('step', {k: v for k, v in step.items() if k != 'command'})

    def execute(self):
        start = time.monotonic()
        status = 'Concluida'
        try:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            for step in self.steps:
                if self.cancelled.is_set():
                    status = 'Cancelada'
                    break
                step.update(status='Executando', started_at=datetime.now().isoformat(timespec='seconds'))
                step_start = time.monotonic()
                self.update(step)
                try:
                    if self.config['simulate']:
                        step['message'] = 'Simulacao: nenhum script ou banco executado.'
                        time.sleep(0.3)
                    elif step['phase'] == 'Criticas':
                        self.criticas(step)
                    else:
                        self.script(step, step_start)
                    step['status'] = 'Simulado' if self.config['simulate'] else ('Com falhas' if step.get('failed_records', 0) else 'Concluido')
                except Exception as exc:
                    status = 'Cancelada' if self.cancelled.is_set() else 'Erro'
                    step.update(status='Cancelado' if self.cancelled.is_set() else 'Erro', message=redact(str(exc), self.config))
                finally:
                    step.update(ended_at=datetime.now().isoformat(timespec='seconds'), seconds=round(time.monotonic() - step_start, 2))
                    self.update(step)
                if status != 'Concluida':
                    break
            if self.cancelled.is_set():
                status = 'Cancelada'
            for step in self.steps:
                if step['status'] == 'Pendente':
                    step['status'] = 'Nao executado'
                    self.update(step)
        except Exception as exc:
            status = 'Erro'
            self.emit('error', redact(str(exc), self.config))
        finally:
            with self.lock:
                process = self.process
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            try:
                if self.config['simulate'] and status == 'Concluida':
                    status = 'Simulada'
                elif status == 'Concluida' and any(step['status'] == 'Com falhas' for step in self.steps):
                    status = 'Concluida com falhas'
                report = self.report(status, time.monotonic() - start)
                self.emit('done', dict(status=status, report=str(report), folder=str(self.run_dir)))
            except Exception as exc:
                self.emit('done', dict(status='Erro no relatorio', report='', folder=str(self.run_dir), message=redact(str(exc), self.config)))

    def script(self, step, started):
        logs = self.run_dir / 'logs'
        logs.mkdir(exist_ok=True)
        detail = logs / f"{step['id']}_{step['company']}_{step['phase']}_{step['group']}.log"
        console = detail.with_suffix('.console.log')
        step['log'] = str(detail)
        if step['phase'] == 'Exportacao':
            Path(step['parquet']).parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.update(MIGRACAO_LOG_FILE=str(detail), PYTHONIOENCODING='utf-8', PYTHONUNBUFFERED='1')
        offset = 0
        pending = ''
        with console.open('w', encoding='utf-8') as output:
            with self.lock:
                if self.cancelled.is_set():
                    raise RuntimeError('Cancelado antes de iniciar o script.')
                self.process = subprocess.Popen(step['command'], cwd=self.config['script_dir'], env=env, stdout=output,
                                                stderr=subprocess.STDOUT, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            while True:
                if detail.exists():
                    with detail.open(encoding='utf-8', errors='replace') as source:
                        source.seek(offset)
                        pending += source.read()
                        offset = source.tell()
                    lines = pending.split('\n')
                    pending = lines.pop()
                    for line in lines:
                        update_counts(step, line)
                        if line.strip():
                            self.emit('log', redact(line, self.config))
                step['seconds'] = round(time.monotonic() - started, 2)
                self.update(step)
                code = self.process.poll()
                if code is not None:
                    # The process has flushed its final log lines before exiting.
                    if detail.exists():
                        with detail.open(encoding='utf-8', errors='replace') as source:
                            source.seek(offset)
                            tail = pending + source.read()
                        for line in tail.splitlines():
                            update_counts(step, line)
                            if line.strip():
                                self.emit('log', redact(line, self.config))
                    break
                if self.cancelled.wait(0.5):
                    with self.lock:
                        if self.process.poll() is None:
                            self.process.terminate()
                    try:
                        self.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
            with self.lock:
                self.process = None
        if self.cancelled.is_set():
            raise RuntimeError('Execucao interrompida. Dados ja gravados nao foram revertidos.')
        if code != 0:
            tail_path = detail if detail.exists() else console
            tail = tail_path.read_text(encoding='utf-8', errors='replace')[-2500:]
            raise RuntimeError(f'Script retornou codigo {code}.\n{tail}')
        if step['phase'] == 'Exportacao':
            if not Path(step['parquet']).exists():
                step.update(records=0, message='Exportador terminou sem criar Parquet (consulte o log).', partial=False)
            else:
                step.update(records=parquet_count(step['parquet']), partial=False)
        else:
            step['partial'] = False
            if step.get('failed_records', 0):
                step['message'] = f"{step['failed_records']} registros com falha; consulte a pasta de erros."

    def criticas(self, step):
        import pyodbc
        writer = module_from(self.config['script_dir'], 'migracao_criticas_report.py').write_criticas_report
        path = self.run_dir / 'criticas.html'
        tmp = path.with_suffix('.tmp')
        conn = pyodbc.connect(odbc_string(self.config['target']), timeout=15)
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.timeout = 600
            with self.lock:
                self.critical_cursor = cursor
            if self.cancelled.is_set():
                raise RuntimeError('Cancelado antes de processar criticas.')
            cursor.execute('EXEC processar_migracao_critica @migracao_cnpj=?, @listar_criticas=1', re.sub(r'\D', '', self.config['cnpj']))
            class CountingCursor:
                total = 0

                def __getattr__(self, name):
                    return getattr(cursor, name)

                def fetchmany(self, size):
                    rows = cursor.fetchmany(size)
                    self.total += len(rows)
                    return rows

            counted = CountingCursor()
            with tmp.open('w', encoding='utf-8') as report:
                writer(counted, self.config['cnpj'], report)
            if self.cancelled.is_set():
                conn.rollback()
                raise RuntimeError('Cancelado durante o processamento de criticas.')
            conn.commit()
            tmp.replace(path)
            step.update(log=str(path), message='Relatorio de criticas gerado.', partial=False, records=counted.total)
        except Exception:
            conn.rollback()
            tmp.unlink(missing_ok=True)
            raise
        finally:
            with self.lock:
                self.critical_cursor = None
            if cursor is not None:
                cursor.close()
            conn.close()

    def report(self, status, seconds):
        safe_steps = [{k: redact(v, self.config) if isinstance(v, str) else v for k, v in step.items() if k != 'command'} for step in self.steps]
        artifacts = []
        for step in self.steps:
            if step['phase'] == 'Exportacao' and step['status'] == 'Concluido' and Path(step['parquet']).is_file():
                path = Path(step['parquet'])
                artifacts.append(dict(company=step['company'], group=step['group'], path=str(path), rows=parquet_count(path), bytes=path.stat().st_size))
        data = dict(status=status, seconds=round(seconds, 2), created_at=datetime.now().isoformat(timespec='seconds'),
                    config=public_config(self.config), steps=safe_steps, parquets=artifacts)
        (self.run_dir / 'execucao.json').write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        esc = lambda value: html.escape(str(value))
        rows = ''.join('<tr>' + ''.join(f'<td>{esc(value)}</td>' for value in (
            s['company'], s['phase'], s['group'], s['status'], s['started_at'], s['seconds'],
            ('~' if s['partial'] else '') + str(s['records']), s.get('read_records', 0),
            s.get('failed_records', 0), s['message'])) + '</tr>' for s in safe_steps)
        files = ''.join(f'<li><a href="{Path(a["path"]).as_uri()}">{esc(a["group"])} / {esc(a["company"])}</a>: {a["rows"]:,} registros, {a["bytes"]:,} bytes</li>' for a in artifacts)
        logs = ''.join(f'<li><a href="{Path(s["log"]).as_uri()}">{esc(s["phase"])} / {esc(s["group"])} / {esc(s["company"])}</a></li>' for s in safe_steps if s['log'])
        filters = ''.join(f'<li>{esc(key)}: {esc(value or "Sem filtro")}</li>' for key, value in self.config['filters'].items())
        page = f'''<!doctype html><html lang="pt-BR"><meta charset="utf-8"><title>Relatorio da migracao</title>
<style>body{{font:14px Segoe UI,Arial;margin:32px;color:#202a2d}}h1{{font-size:26px}}table{{border-collapse:collapse;width:100%}}th,td{{padding:10px;border:1px solid #d6dfe0;text-align:left;vertical-align:top;white-space:pre-wrap}}th{{background:#eaf2ef}}a{{color:#117355}}.scroll{{overflow:auto}}</style>
<h1>Relatorio da migracao</h1><p>{esc(status)} | {seconds:.1f}s | {esc(data['created_at'])}</p>
<p>Contagens de exportacao: metadados finais do Parquet. Importacao: insercoes confirmadas nos logs; deduplicacoes posteriores podem reduzir o total no destino. ~ indica contagem parcial.</p>
<div class="scroll"><table><tr><th>Company</th><th>Etapa</th><th>Grupo</th><th>Status</th><th>Inicio</th><th>Segundos</th><th>Gerados / Inseridos / Retornos</th><th>Lidos (importacao)</th><th>Falhas</th><th>Observacao</th></tr>{rows}</table></div>
<h2>Parquets gerados nesta execucao</h2><ul>{files or '<li>Nenhum.</li>'}</ul><h2>Logs e criticas</h2><ul>{logs or '<li>Nenhum.</li>'}</ul><h2>Filtros</h2><ul>{filters}</ul>
<p>CNPJ: {esc(self.config['cnpj'])}. Limpeza: {esc(self.config['cleanup'])}. Inativos: {esc(self.config['considerar_inactive'])}.</p>
<p><a href="{(self.run_dir / 'erros').as_uri()}">Pasta de erros de importacao</a></p></html>'''
        path = self.run_dir / 'relatorio.html'
        path.write_text(page, encoding='utf-8')
        return path
