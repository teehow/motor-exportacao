# Painel de Migracao Bex

Aplicativo Windows em Tkinter que executa os scripts existentes, sem reimplementar os SQLs de exportacao.

## Abrir

Abra `Abrir Painel Migracao.cmd` com dois cliques. Alternativamente, execute `Iniciar Painel Migracao.ps1` na pasta acima ou:

```powershell
# Dentro da pasta extraida do pacote, depois de executar Instalar.cmd:
& ".\.venv\Scripts\python.exe" ".\painel_migracao\app.py"
```

O pacote usa Python 3.12 ou superior, 64 bits, com Tkinter. `Instalar.cmd` cria um ambiente virtual local e instala as dependencias de `requirements.txt`. O driver ODBC do SQL Server deve ser instalado no Windows separadamente. Os caminhos padrao apontam para `motor`, `dados` e `saida` dentro da propria pasta do pacote.

## Configuracao

- Modos: completa, somente exportacao ou somente importacao; grupos executados na ordem do motor.
- Conexoes PostgreSQL e SQL Server configuraveis, incluindo portas, SSL da origem e driver do destino. Os testes de conexao executam somente SELECT.
- Filtros CSV de projetos, pendencias, contratadas e project_company_account; de/para do contrato por project_id e arquivos de grupos/areas.
- Flag de inativos, lotes, fetch, blocos de projetos, compressoes e fallback da importacao.
- Company IDs separados por virgula ou espaco. Para criticas, informe um CNPJ e marque a opcao; uma chamada da procedure ocorre ao final de todas as importacoes.
- Perfis JSON nao armazenam senhas. Digite-as novamente ao abrir um perfil.

## Execucao e seguranca

A configuracao fica bloqueada enquanto uma execucao esta ativa. A tabela mostra inicio, duracao, status e registros de cada etapa. A contagem precedida de `~` e parcial; a quantidade final de exportacao e lida dos metadados do Parquet. Na importacao, a quantidade corresponde as insercoes informadas nos logs, antes das deduplicacoes pos-importacao do motor.

Os nove exportadores aplicam uma subtracao fixa de 3 horas aos campos de data e hora antes da escrita. Campos somente de data, valores nulos e horarios sem data sao preservados. Nao ha ajuste adicional na importacao.

Cada exportacao usa uma pasta nova em `output/execucoes/DATA_HORA/company_ID`. Nao sobrescreve Parquets de execucoes anteriores. Para somente importar, selecione a pasta que contem os Parquets; para varias empresas, selecione a pasta-pai das pastas `company_ID`.

A limpeza e desmarcada por padrao. Quando desmarcada, o CNPJ nao e passado ao importador, pois o motor interpreta o CNPJ como solicitacao de limpeza. O CNPJ continua disponivel para a procedure de criticas. Quando marcada, a limpeza exige confirmacao, segue as regras atuais do motor e ocorre apenas para a primeira empresa da lista, como no executor original.

Cancelar interrompe o processo ativo; dados ja gravados nao sao revertidos. Durante a procedure de criticas, o cancelamento solicita o cancelamento da consulta e faz rollback se ainda nao houve commit. Nunca feche a aplicacao antes da finalizacao do cancelamento.

Os comandos de subprocessos precisam fornecer senhas ao motor existente, mas o painel nao os grava nos perfis ou relatorios. Os logs detalhados dos scripts continuam sendo arquivos locais com os retornos do motor; trate-os como dados confidenciais.

## Relatorios

Cada execucao gera `relatorio.html` e `execucao.json`, inclusive em caso de erro ou cancelamento. O HTML lista as etapas, duracoes, Parquets efetivamente gerados, registros, tamanhos e links para logs e criticas. Os totais finais de importacao incluem linhas lidas, preparadas, inseridas e com falhas, extraidos do resumo do proprio motor. Etapas com fallback que terminem com registros rejeitados ficam marcadas como `Com falhas`. A procedure gera `criticas.html` com todos os conjuntos de resultados recebidos.

O modo Simulacao valida filtros e monta as etapas sem iniciar os exportadores, o importador ou a procedure. Nao confirma conectividade ou conteudo real dos bancos.

Para selecao de um grupo, o importador original foi ajustado para validar a existencia apenas dos Parquets selecionados por `--only`. Seus atributos ODBC passaram a ser escapados para aceitar senhas com `;` e `}`. Os SQLs e regras de importacao nao foram alterados.
