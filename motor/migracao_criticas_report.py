from datetime import datetime
from html import escape


def write_criticas_report(cursor, cnpj, report):
    report.write(
        '<!doctype html><html lang="pt-BR"><meta charset="utf-8">'
        '<title>Criticas da migracao</title><style>'
        'body{font:14px Arial,sans-serif;margin:24px;color:#202020}'
        'table{border-collapse:collapse;margin:12px 0;width:100%}'
        'th,td{border:1px solid #ccc;padding:8px;text-align:left;vertical-align:top;white-space:pre-wrap}'
        'th{background:#eee}h1{font-size:24px}h2{font-size:18px}'
        '</style><h1>Criticas da migracao</h1>'
    )
    report.write(f'<p>CNPJ: {escape(cnpj)}<br>Gerado em: {datetime.now():%d/%m/%Y %H:%M:%S}</p>')
    result_number = 0
    while True:
        if cursor.description is not None:
            result_number += 1
            columns = [column[0] for column in cursor.description]
            report.write(f'<h2>Retorno {result_number}</h2><table><thead><tr>')
            for column in columns:
                report.write(f'<th>{escape(str(column))}</th>')
            report.write('</tr></thead><tbody>')
            row_count = 0
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    row_count += 1
                    report.write('<tr>')
                    for value in row:
                        report.write(f'<td>{escape("NULL" if value is None else str(value))}</td>')
                    report.write('</tr>')
            report.write(f'</tbody></table><p>Total de registros: {row_count}</p>')
        for message in cursor.messages:
            report.write(f'<p>SQL Server: {escape(str(message[1]))}</p>')
        if not cursor.nextset():
            break
    if result_number == 0:
        report.write('<p>Procedure concluida sem conjuntos de resultados.</p>')
    report.write('</html>')
