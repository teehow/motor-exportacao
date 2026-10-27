# Migracao Bex - pacote Windows 1.0.0

## Primeiro uso em outro computador

1. Extraia todo o ZIP em uma pasta local com permissao de escrita. Nao execute de dentro do ZIP.
2. Instale Python 3.12 ou superior, 64 bits, incluindo Tcl/Tk e o launcher `py`.
3. Para importar no SQL Server ou processar criticas, instale o Microsoft ODBC Driver for SQL Server, 64 bits. Se usar Driver 18, selecione-o na interface.
4. Execute `Instalar.cmd`. A primeira instalacao precisa de internet para baixar as bibliotecas Python. Nao e necessario copiar o ambiente virtual do computador de origem.
5. Execute `Abrir Painel Migracao.cmd`.
6. Informe company_id, conexoes e arquivos de filtro/mapeamento. Nenhum banco, usuario ou senha real vem preenchido.
7. Use Simulacao para conferir a configuracao, depois Testar conexao e, quando tudo estiver correto, Iniciar sem Simulacao.

Este pacote e uma aplicacao Python com instalacao de dependencias, nao um executavel independente de Python. Ele nao cria tabelas ou procedures. O destino precisa ter a estrutura de migracao ja preparada, inclusive `processar_migracao_critica` quando essa opcao for usada.

## Conteudo

- `motor/`: scripts Python refatorados, SQLs e utilitarios do motor.
- `painel_migracao/`: interface e acompanhamento de execucoes.
- `dados/exemplos/`: CSVs ficticios para demonstrar os formatos. Nao sao filtros ativos.
- `saida/`: pasta criada durante o uso para dados gerados, logs e relatorios.
- `requirements.txt`: versoes das dependencias verificadas no ambiente de desenvolvimento.
- `MANIFESTO.json`: SHA-256 e tamanho dos arquivos entregues.
- `Verificar Ambiente.cmd`: verifica bibliotecas, drivers e integridade dos arquivos, sem consultar bancos.

Nao fazem parte do pacote: senhas, perfis reais, CSVs de clientes, planilhas, Parquets, logs, relatorios anteriores, arquivos de backup, ambiente virtual ou scripts obsoletos.

## Nomes dos scripts

| Grupo | Arquivo |
| --- | --- |
| Operadores | `motor/exportar_operadores.py` |
| Contratos | `motor/exportar_contratos.py` |
| Colaboradores | `motor/exportar_colaboradores.py` |
| Analises | `motor/exportar_analises.py` |
| Procedimentos | `motor/exportar_procedimentos.py` |
| Riscos | `motor/exportar_riscos.py` |
| Blacklist | `motor/exportar_blacklist.py` |
| Integracoes | `motor/exportar_integracoes.py` |
| Vinculos de operadores | `motor/exportar_operadores_vinculos.py` |
| Importacao e validacao | `motor/importar_validar.py` |
| Executor completo por terminal | `motor/executar_migracao.py` |

Os numeros de chamados e de empresa foram retirados dos nomes. As consultas e regras dos exportadores/importador foram preservadas. Os arquivos antigos na pasta original de desenvolvimento nao sao excluidos nem distribuidos.

## Filtros e mapeamentos

Use os arquivos corretos do cliente; consulte `dados/LEIA-ME.md`. Filtros opcionais em branco nao restringem a exportacao. `grupoAreas.csv` e exigido em contratos e `grupos.csv` em riscos. Copie os seus arquivos para `dados` ou selecione outra pasta no painel.

O CSV de troca de contratos usa `project_id;contrato_numero;project.name;contratada`, com a quarta coluna opcional. O CSV de operadores usa `project_company_account.id`, sem exigir PCA.deleted=false para os IDs selecionados. Os demais filtros existentes foram mantidos.

## Execucao e relatorios

O painel permite execucao completa, exportacao ou importacao, com selecao de grupos. Cada execucao tem uma pasta propria e gera relatorios HTML e JSON. Contagens parciais sao indicadas com `~`; a contagem final de exportacao vem dos metadados do Parquet.

Para somente importar, selecione a pasta dos Parquets existentes. Para varias empresas, ela deve conter subpastas `company_ID`.

Limpeza do destino e desmarcada por padrao e exige confirmacao. O painel so envia o CNPJ ao importador quando a limpeza esta marcada. O mesmo CNPJ pode ser usado para criticas sem solicitar limpeza. Cancelar nao reverte dados ja gravados. Logs e relatorios podem conter dados pessoais do cliente e devem ser protegidos.

Perfis JSON nao guardam senhas. Informe as senhas novamente na interface. Nao compartilhe a pasta de trabalho inteira depois de usa-la: redistribua o ZIP original limpo ou gere outro com `empacotar.py`.

## Uso pelo terminal

O painel e a forma recomendada. Para conferir os parametros dos scripts:

```powershell
& ".\.venv\Scripts\python.exe" ".\motor\exportar_contratos.py" --help
& ".\.venv\Scripts\python.exe" ".\motor\importar_validar.py" --help
& ".\Executar Migracao.cmd" --help
```

No CMD, use `"Executar Migracao.cmd" --help`. No PowerShell, use `& ".\Executar Migracao.cmd" --help`.

O executor completo aceita `--source-host`, `--source-port`, `--source-db`, `--source-user`, `--source-sslmode` e os equivalentes `--sqlserver-*`. Senhas podem ser informadas por `BEX_SOURCE_PASSWORD` e `GRT_MIGRACAO_PASSWORD`, evitando inclui-las na linha de comando. `--dry-run` oculta as senhas ao mostrar comandos.

Atencao: no executor de terminal, o comportamento legado de `--contratante-cnpj` inclui limpeza do contratante no importador. No painel, a limpeza e controlada pela opcao explicita de limpeza.

## Recriar a distribuicao

```powershell
python empacotar.py
```

O gerador usa uma lista explicita de arquivos e nao inclui dados adicionados posteriormente, configuracoes de usuario, `.venv`, `saida`, caches ou `dist`. O ZIP e seu SHA-256 sao gravados em `dist`.
