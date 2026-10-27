from __future__ import annotations

import json
import os
import queue
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from core import GROUPS, MigrationRun, defaults, load_profile, public_config, redact, test_connection, validate


def open_path(path):
    path = Path(path)
    if not path.exists():
        raise ValueError(f'Arquivo ou pasta nao encontrado: {path}')
    os.startfile(str(path.resolve()))


def duration(seconds):
    seconds = int(seconds)
    return f'{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}'


class ScrollPage(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.canvas = tk.Canvas(self, background='#f4f7f6', highlightthickness=0)
        bar = ttk.Scrollbar(self, orient='vertical', command=self.canvas.yview)
        self.body = ttk.Frame(self.canvas, padding=20)
        self.window = self.canvas.create_window((0, 0), window=self.body, anchor='nw')
        self.body.bind('<Configure>', lambda event: self.canvas.configure(scrollregion=self.canvas.bbox('all')))
        self.canvas.bind('<Configure>', lambda event: self.canvas.itemconfigure(self.window, width=event.width))
        self.canvas.configure(yscrollcommand=bar.set)
        self.canvas.pack(side='left', fill='both', expand=True)
        bar.pack(side='right', fill='y')
        self.canvas.bind('<Enter>', lambda event: self.canvas.bind_all('<MouseWheel>', self.wheel))
        self.canvas.bind('<Leave>', lambda event: self.canvas.unbind_all('<MouseWheel>'))

    def wheel(self, event):
        self.canvas.yview_scroll(int(-event.delta / 120), 'units')


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Migracao Bex | Controle de execucao')
        self.geometry('1220x860')
        self.minsize(900, 650)
        self.configure(background='#f4f7f6')
        style = ttk.Style(self)
        style.theme_use('clam')
        style.configure('.', font=('Segoe UI', 10), background='#f4f7f6', foreground='#233238')
        style.configure('TFrame', background='#f4f7f6')
        style.configure('TLabel', background='#f4f7f6')
        style.configure('TNotebook', background='#f4f7f6', borderwidth=0)
        style.configure('TNotebook.Tab', padding=(18, 10))
        style.map('TNotebook.Tab', background=[('selected', '#ffffff')])
        style.configure('TEntry', fieldbackground='white', padding=6)
        style.configure('TCombobox', padding=6)
        style.configure('TButton', padding=(12, 7))
        style.configure('Primary.TButton', background='#13795b', foreground='white', font=('Segoe UI', 10, 'bold'))
        style.map('Primary.TButton', background=[('disabled', '#a3bcb3'), ('active', '#0c6348')])
        style.configure('Title.TLabel', font=('Segoe UI', 21, 'bold'))
        style.configure('Section.TLabel', font=('Segoe UI', 12, 'bold'))
        style.configure('Muted.TLabel', foreground='#5e7179')
        style.configure('Treeview', rowheight=34, fieldbackground='white', background='white')
        style.configure('Treeview.Heading', padding=8, font=('Segoe UI', 10, 'bold'))
        style.configure('Horizontal.TProgressbar', background='#13795b', troughcolor='#dce7e2')
        self.values = {}
        self.events = queue.Queue()
        self.run = None
        self.active = False
        self.started = 0
        self.latest_report = ''
        self.run_folder = ''
        self.widgets = []
        self.rows = {}
        self._build()
        self.set_config(defaults())
        self.poll_job = self.after(150, self.poll)
        self.protocol('WM_DELETE_WINDOW', self.close)

    def var(self, key, boolean=False):
        if key not in self.values:
            self.values[key] = tk.BooleanVar(self) if boolean else tk.StringVar(self)
        return self.values[key]

    def field(self, parent, label, key, row, *, browse=None, options=None, password=False):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky='w', pady=7, padx=(0, 16))
        if options:
            widget = ttk.Combobox(parent, textvariable=self.var(key), values=options, state='readonly', width=30)
        else:
            widget = ttk.Entry(parent, textvariable=self.var(key), show='*' if password else '', width=34)
        widget.grid(row=row, column=1, sticky='ew', pady=7)
        self.widgets.append((widget, 'readonly' if options else 'normal'))
        parent.columnconfigure(1, weight=1)
        if browse:
            button = ttk.Button(parent, text='...', width=3, command=lambda: self.browse(key, browse))
            button.grid(row=row, column=2, padx=(7, 0))
            self.widgets.append((button, 'normal'))
        return widget

    def check(self, parent, label, key, row, column=0, span=2):
        widget = ttk.Checkbutton(parent, text=label, variable=self.var(key, True))
        widget.grid(row=row, column=column, columnspan=span, sticky='w', pady=7, padx=(0, 18))
        self.widgets.append((widget, 'normal'))
        return widget

    def section(self, parent, text, row, span=3):
        ttk.Label(parent, text=text, style='Section.TLabel').grid(row=row, column=0, columnspan=span, sticky='w', pady=(18, 8))

    def _build(self):
        header = ttk.Frame(self, padding=(24, 18))
        header.pack(fill='x')
        title = ttk.Frame(header)
        title.pack(side='left')
        ttk.Label(title, text='Migracao Bex', style='Title.TLabel').pack(anchor='w')
        ttk.Label(title, text='PostgreSQL  >  SQL Server', style='Muted.TLabel').pack(anchor='w', pady=(3, 0))
        self.start_button = ttk.Button(header, text='Iniciar', style='Primary.TButton', command=self.start)
        self.start_button.pack(side='right', padx=(8, 0))
        self.cancel_button = ttk.Button(header, text='Cancelar', command=self.cancel, state='disabled')
        self.cancel_button.pack(side='right')
        self.tabs = ttk.Notebook(self)
        self.tabs.pack(fill='both', expand=True, padx=20)
        config = ttk.Frame(self.tabs)
        execution = ttk.Frame(self.tabs, padding=16)
        reports = ttk.Frame(self.tabs, padding=20)
        self.tabs.add(config, text='Configuracao')
        self.tabs.add(execution, text='Execucao')
        self.tabs.add(reports, text='Relatorios')
        self.config_tabs = ttk.Notebook(config)
        self.config_tabs.pack(fill='both', expand=True, pady=(8, 0))
        pages = {}
        for label in ('Migracao', 'Conexoes', 'Filtros e arquivos', 'Desempenho'):
            page = ScrollPage(self.config_tabs)
            self.config_tabs.add(page, text=label)
            pages[label] = page.body
        self.general(pages['Migracao'])
        self.connections(pages['Conexoes'])
        self.filters(pages['Filtros e arquivos'])
        self.tuning(pages['Desempenho'])
        toolbar = ttk.Frame(config, padding=(0, 10))
        toolbar.pack(fill='x')
        for label, action in (('Abrir perfil', self.load), ('Salvar perfil', self.save)):
            button = ttk.Button(toolbar, text=label, command=action)
            button.pack(side='left', padx=(0, 8))
            self.widgets.append((button, 'normal'))
        self.state_text = tk.StringVar(value='Pronto para configurar')
        ttk.Label(execution, textvariable=self.state_text, style='Section.TLabel').pack(anchor='w', pady=(0, 12))
        columns = ('company', 'phase', 'group', 'status', 'started', 'elapsed', 'records')
        tree_frame = ttk.Frame(execution)
        tree_frame.pack(fill='both', expand=True)
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=10, selectmode='browse')
        for key, label, width in zip(columns, ('Company', 'Etapa', 'Grupo', 'Status', 'Inicio', 'Tempo', 'Registros'), (78, 108, 160, 118, 96, 82, 120)):
            self.tree.heading(key, text=label)
            self.tree.column(key, width=width, minwidth=65, anchor='e' if key == 'records' else 'w')
        bar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=bar.set)
        self.tree.pack(side='left', fill='both', expand=True)
        bar.pack(side='right', fill='y')
        self.tree.tag_configure('Executando', background='#e6f3ee')
        self.tree.tag_configure('Concluido', foreground='#0d6a4e')
        self.tree.tag_configure('Erro', background='#fbeceb', foreground='#a12b28')
        self.tree.tag_configure('Com falhas', background='#fff0d5', foreground='#865313')
        self.tree.bind('<Double-1>', lambda event: self.open_log())
        actions = ttk.Frame(execution)
        actions.pack(fill='x', pady=8)
        ttk.Button(actions, text='Abrir log selecionado', command=self.open_log).pack(side='left')
        ttk.Label(execution, text='Eventos', style='Section.TLabel').pack(anchor='w', pady=(10, 6))
        logs = ttk.Frame(execution)
        logs.pack(fill='both', expand=True)
        self.log_text = tk.Text(logs, height=8, wrap='word', font=('Consolas', 9), background='#ffffff', foreground='#344349', relief='flat', padx=10, pady=10, state='disabled')
        log_bar = ttk.Scrollbar(logs, orient='vertical', command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_bar.set)
        self.log_text.pack(side='left', fill='both', expand=True)
        log_bar.pack(side='right', fill='y')
        self.progress = ttk.Progressbar(self, mode='determinate')
        footer = ttk.Frame(self, padding=(22, 10))
        footer.pack(side='bottom', fill='x', before=self.tabs)
        self.progress.pack(side='bottom', fill='x', padx=20, pady=(12, 0), before=self.tabs)
        self.footer = tk.StringVar(value='Nenhuma execucao iniciada')
        self.clock = tk.StringVar(value='00:00:00')
        ttk.Label(footer, textvariable=self.footer, style='Muted.TLabel', wraplength=850).pack(side='left')
        ttk.Label(footer, textvariable=self.clock).pack(side='right')
        ttk.Label(reports, text='Relatorios de execucao', style='Section.TLabel').pack(anchor='w', pady=(0, 12))
        self.report_list = tk.Listbox(reports, background='white', borderwidth=0, font=('Segoe UI', 11), activestyle='none')
        self.report_list.pack(fill='both', expand=True)
        self.report_list.bind('<Double-1>', lambda event: self.open_report())
        self.report_paths = []
        report_actions = ttk.Frame(reports, padding=(0, 12))
        report_actions.pack(fill='x')
        for label, action in (('Abrir relatorio', self.open_report), ('Abrir pasta', self.open_folder), ('Atualizar', self.refresh_reports)):
            ttk.Button(report_actions, text=label, command=action).pack(side='left', padx=(0, 8))

    def general(self, body):
        self.field(body, 'Modo', 'mode', 0, options=('Completa', 'Somente exportacao', 'Somente importacao'))
        self.field(body, 'Company ID(s)', 'company_ids', 1)
        self.field(body, 'CNPJ do contratante', 'cnpj', 2)
        self.section(body, 'Grupos de dados', 3)
        groups = ttk.Frame(body)
        groups.grid(row=4, column=0, columnspan=3, sticky='ew')
        for index, group in enumerate(GROUPS):
            self.check(groups, group.replace('_', ' ').capitalize(), 'group.' + group, index // 3, index % 3, 1)
        self.section(body, 'Execucao', 5)
        self.check(body, 'Considerar pendencias inativadas', 'considerar_inactive', 6)
        self.check(body, 'Limpar registros existentes no destino', 'cleanup', 7)
        self.check(body, 'Processar criticas ao final', 'criticas', 8)
        self.check(body, 'Simulacao (sem executar scripts ou bancos)', 'simulate', 9)

    def connections(self, body):
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        for column, (section, title) in enumerate((('source', 'Origem | PostgreSQL'), ('target', 'Destino | SQL Server'))):
            frame = ttk.Frame(body, padding=(0, 0, 24 if column == 0 else 0, 0))
            frame.grid(row=0, column=column, sticky='new')
            self.section(frame, title, 0)
            for row, (label, field) in enumerate((('Servidor', 'host'), ('Porta', 'port'), ('Banco', 'database'), ('Usuario', 'user'), ('Senha', 'password')), 1):
                self.field(frame, label, section + '.' + field, row, password=field == 'password')
            if section == 'source':
                self.field(frame, 'SSL', 'source.sslmode', 6, options=('disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'))
            else:
                try:
                    import pyodbc
                    drivers = pyodbc.drivers()
                except ImportError:
                    drivers = ['ODBC Driver 17 for SQL Server']
                self.field(frame, 'Driver ODBC', 'target.driver', 6, options=drivers)
            button = ttk.Button(frame, text='Testar conexao', command=lambda value=section: self.test_db(value))
            button.grid(row=7, column=1, sticky='w', pady=12)
            self.widgets.append((button, 'normal'))
            self.var(section + '.status').set('Nao testada')
            ttk.Label(frame, textvariable=self.var(section + '.status'), style='Muted.TLabel', wraplength=310).grid(row=8, column=0, columnspan=3, sticky='w')

    def filters(self, body):
        self.section(body, 'Diretorios e interpretador', 0)
        for row, (label, key, kind) in enumerate((('Python da migracao', 'python', 'python'), ('Scripts do motor', 'script_dir', 'dir'), ('Saida e relatorios', 'output_dir', 'dir'), ('Parquets para importacao', 'import_dir', 'dir')), 1):
            self.field(body, label, key, row, browse=kind)
        self.section(body, 'Filtros opcionais (CSV)', 5)
        labels = ('Projetos | project.name', 'Pendencias | pendency.pendency_id', 'Contratadas | company.cnpj', 'Operadores | project_company_account.id', 'Troca do numero do contrato | project_id')
        for row, (key, label) in enumerate(zip(('project_data', 'pendency_data', 'contratada_data', 'project_company_account_data', 'troca_contrato_numero'), labels), 6):
            self.field(body, label, 'filters.' + key, row, browse='csv')
        self.section(body, 'Mapeamentos (CSV)', 11)
        self.field(body, 'Grupo e area de contratos', 'grupoareas_csv', 12, browse='csv')
        self.field(body, 'Grupo de riscos', 'grupos_csv', 13, browse='csv')

    def tuning(self, body):
        fields = (
            ('Analises', (('Lote de escrita', 'analises_batch_size'), ('Fetch PostgreSQL', 'analises_fetch_size'), ('Projetos por bloco', 'analises_project_block_size'), ('Compressao', 'analises_parquet_compression'))),
            ('Colaboradores', (('Projetos por bloco', 'colaboradores_project_block_size'),)),
            ('Procedimentos', (('Lote de escrita', 'procedimentos_batch_size'), ('Compressao', 'procedimentos_parquet_compression'))),
            ('Riscos', (('Lote de leitura/escrita', 'riscos_chunksize'), ('Projetos por bloco', 'riscos_project_block_size'))),
            ('Demais exportacoes', (('Tamanho de lote', 'export_chunksize'), ('Compressao disponivel', 'export_compression'))),
            ('Importacao SQL Server', (('Tamanho de lote', 'import_chunksize'),)),
        )
        row = 0
        for label, options in fields:
            self.section(body, label, row)
            row += 1
            for field, key in options:
                self.field(body, field, 'tuning.' + key, row, options=('snappy', 'gzip', 'zstd', 'none') if key.endswith('compression') else None)
                row += 1
        self.check(body, 'Fallback de importacao linha a linha', 'row_fallback', row)

    def browse(self, key, kind):
        if kind == 'dir':
            value = filedialog.askdirectory(parent=self, initialdir=self.var(key).get() or str(Path.home()))
        else:
            value = filedialog.askopenfilename(parent=self, filetypes=[('CSV', '*.csv')] if kind == 'csv' else [('Python', '*.exe'), ('Todos', '*.*')])
        if value:
            self.var(key).set(value)

    def set_config(self, config):
        for key, value in config.items():
            if isinstance(value, dict):
                for child, item in value.items():
                    self.var(key + '.' + child).set(item)
            elif key == 'groups':
                for group in GROUPS:
                    self.var('group.' + group, True).set(group in value)
            else:
                self.var(key, isinstance(value, bool)).set(value)
        self.refresh_reports()

    def collect(self):
        config = defaults()
        for key, value in config.items():
            if isinstance(value, dict):
                for child, item in value.items():
                    raw = self.var(key + '.' + child).get()
                    try:
                        config[key][child] = int(raw) if isinstance(item, int) else raw
                    except ValueError:
                        raise ValueError(f'Valor numerico invalido: {child}') from None
            elif key == 'groups':
                config[key] = [group for group in GROUPS if self.var('group.' + group, True).get()]
            else:
                config[key] = self.var(key, isinstance(value, bool)).get()
        return config

    def save(self):
        try:
            config = public_config(self.collect())
            path = filedialog.asksaveasfilename(parent=self, defaultextension='.json', filetypes=[('Perfil', '*.json')])
            if path:
                Path(path).write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding='utf-8')
                self.footer.set('Perfil salvo sem senhas')
        except Exception as exc:
            messagebox.showerror('Perfil', str(exc), parent=self)

    def load(self):
        path = filedialog.askopenfilename(parent=self, filetypes=[('Perfil', '*.json')])
        if path:
            try:
                self.set_config(load_profile(path))
            except Exception as exc:
                messagebox.showerror('Perfil', str(exc), parent=self)

    def test_db(self, section):
        try:
            config = self.collect()
        except ValueError as exc:
            messagebox.showerror('Conexao', str(exc), parent=self)
            return
        self.var(section + '.status').set('Testando...')
        def work():
            try:
                text = 'Conectado: ' + test_connection(section, config[section])
            except Exception as exc:
                text = redact(str(exc), config)
            self.events.put(('connection', (section, text)))
        threading.Thread(target=work, daemon=True).start()

    def set_running(self, active):
        self.active = active
        self.start_button.configure(state='disabled' if active else 'normal')
        self.cancel_button.configure(state='normal' if active else 'disabled')
        for widget, original in self.widgets:
            widget.configure(state='disabled' if active else original)

    def start(self):
        if self.active:
            return
        try:
            config = self.collect()
            validate(config)
            if config['cleanup'] and config['mode'] != 'Somente exportacao' and not config['simulate']:
                scope = f"CNPJ {config['cnpj']}" if config['cnpj'] else 'TODOS os contratantes dos grupos selecionados'
                if not messagebox.askyesno('Confirmar limpeza do SQL Server', f'Os registros existentes serao removidos conforme as regras do motor.\nEscopo: {scope}.\n\nDeseja continuar?', parent=self):
                    return
            self.run = MigrationRun(config, lambda kind, value: self.events.put((kind, value)))
        except Exception as exc:
            messagebox.showerror('Verifique a configuracao', str(exc), parent=self)
            return
        self.tree.delete(*self.tree.get_children())
        self.rows.clear()
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', 'end')
        self.log_text.configure(state='disabled')
        self.progress.configure(maximum=len(self.run.steps), value=0)
        for step in self.run.steps:
            self.update_step(step)
        self.set_running(True)
        self.started = time.monotonic()
        self.state_text.set('Execucao em andamento')
        self.footer.set('Iniciando...')
        self.tabs.select(1)
        threading.Thread(target=self.run.execute, daemon=False).start()

    def update_step(self, step):
        self.rows[step['id']] = step
        records = ('~ ' if step['partial'] and step['status'] == 'Executando' else '') + f"{step['records']:,}".replace(',', '.')
        values = (step['company'], step['phase'], step['group'], step['status'], step['started_at'][11:19], duration(step['seconds']), records)
        if self.tree.exists(step['id']):
            self.tree.item(step['id'], values=values, tags=(step['status'],))
        else:
            self.tree.insert('', 'end', iid=step['id'], values=values, tags=(step['status'],))
        if step['status'] == 'Executando':
            self.tree.see(step['id'])
            self.footer.set(f"{step['phase']} | {step['group']} | company {step['company']}")
        self.progress['value'] = sum(s['status'] in ('Concluido', 'Simulado', 'Com falhas') for s in self.rows.values())

    def poll(self):
        for _ in range(300):
            try:
                kind, value = self.events.get_nowait()
            except queue.Empty:
                break
            if kind == 'step':
                self.update_step(value)
            elif kind in ('log', 'error'):
                self.log_text.configure(state='normal')
                self.log_text.insert('end', str(value) + '\n')
                if int(self.log_text.index('end-1c').split('.')[0]) > 800:
                    self.log_text.delete('1.0', '200.0')
                self.log_text.see('end')
                self.log_text.configure(state='disabled')
            elif kind == 'connection':
                section, text = value
                self.var(section + '.status').set(text)
            elif kind == 'done':
                self.set_running(False)
                self.latest_report = value['report']
                self.run_folder = value['folder']
                self.state_text.set('Execucao: ' + value['status'])
                self.footer.set(value.get('message', 'Relatorio: ' + value['folder']))
                self.refresh_reports()
        if self.active:
            self.clock.set(duration(time.monotonic() - self.started))
        self.poll_job = self.after(150, self.poll)

    def cancel(self):
        if self.active and messagebox.askyesno('Cancelar', 'Interromper a execucao? Dados ja gravados nao serao revertidos automaticamente.', parent=self):
            self.cancel_button.configure(state='disabled')
            self.footer.set('Cancelamento solicitado...')
            threading.Thread(target=self.run.cancel, daemon=True).start()

    def open_log(self):
        selected = self.tree.selection()
        if selected:
            path = self.rows[selected[0]].get('log')
            if path:
                self.safe_open(path)

    def refresh_reports(self):
        root = Path(self.var('output_dir').get() or '.') / 'execucoes'
        self.report_paths = sorted(root.glob('*/relatorio.html'), reverse=True) if root.is_dir() else []
        self.report_list.delete(0, 'end')
        for path in self.report_paths:
            label = path.parent.name
            try:
                data = json.loads((path.parent / 'execucao.json').read_text(encoding='utf-8'))
                label = f"{data['created_at'].replace('T', ' ')}  |  {data['status']}  |  {duration(data['seconds'])}"
            except (OSError, ValueError, KeyError):
                pass
            self.report_list.insert('end', label)
        if self.report_paths:
            self.report_list.selection_set(0)

    def open_report(self):
        selected = self.report_list.curselection()
        if selected:
            self.safe_open(self.report_paths[selected[0]])

    def open_folder(self):
        selected = self.report_list.curselection()
        self.safe_open(self.report_paths[selected[0]].parent if selected else self.var('output_dir').get())

    def safe_open(self, path):
        try:
            open_path(path)
        except Exception as exc:
            messagebox.showerror('Abrir', str(exc), parent=self)

    def close(self):
        if self.active:
            messagebox.showwarning('Execucao ativa', 'Cancele a execucao e aguarde o relatorio antes de fechar.', parent=self)
            return
        self.destroy()

    def destroy(self):
        job = getattr(self, 'poll_job', None)
        if job:
            self.after_cancel(job)
            self.poll_job = None
        super().destroy()


if __name__ == '__main__':
    App().mainloop()
