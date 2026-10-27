$ErrorActionPreference = 'Stop'
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) {
    throw 'Execute Instalar.cmd antes de abrir o painel.'
}
& $python (Join-Path $PSScriptRoot 'painel_migracao\app.py')
