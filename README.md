# Motor de Exportacao e Migracao Bex

Fonte oficial do painel Windows e dos scripts de exportacao PostgreSQL e importacao SQL Server.

Repositorio: https://github.com/teehow/motor-exportacao

## Uso

1. Clone o repositorio ou extraia o pacote de distribuicao.
2. Instale Python 3.12 ou superior, 64 bits, com Tcl/Tk. Para usar SQL Server, instale tambem o Microsoft ODBC Driver for SQL Server.
3. Execute `Instalar.cmd` para criar `.venv` e instalar as dependencias.
4. Abra `Abrir Painel Migracao.cmd` e informe conexoes, company_id, filtros e mapeamentos.

Nenhuma senha, perfil de cliente ou conexao real e fornecida. A primeira instalacao das bibliotecas requer internet. O pacote nao e um executavel independente de Python.

Consulte [LEIA-ME.md](LEIA-ME.md) para instalacao, seguranca e uso; [dados/LEIA-ME.md](dados/LEIA-ME.md) para os CSVs; e [painel_migracao/README.md](painel_migracao/README.md) para o painel.

## Organizacao

- `motor/exportar_*.py`: nove grupos de exportacao, com consultas e regras de negocio.
- `motor/importar_validar.py`: importacao, validacao e deduplicacao no SQL Server.
- `motor/executar_migracao.py`: executor completo via terminal.
- `painel_migracao/`: interface, acompanhamento e relatorios.
- `painel_migracao/tests/`: testes de comandos, execucao, interface e distribuicao.
- `dados/exemplos/`: somente exemplos ficticios.
- `empacotar.py`: gerador do ZIP limpo e de seus hashes.

## Desenvolvimento

Altere os scripts refatorados em `motor/`, nao copias antigas de outros diretorios. Este repositorio deve ser a fonte das proximas distribuicoes.

Dentro da pasta do projeto:

```powershell
& ".\.venv\Scripts\python.exe" -m unittest discover -s painel_migracao/tests -v
& ".\.venv\Scripts\python.exe" empacotar.py
```

O ZIP e seu SHA-256 sao gerados em `dist/`. O conteudo do pacote e controlado por uma lista explicita em `empacotar.py`, com um manifesto de integridade por arquivo. Antes de publicar uma nova versao, atualize `VERSION` nesse arquivo e a identificacao da versao em `LEIA-ME.md`.

O teste de equivalencia com fontes legadas e opcional: defina `MOTOR_LEGADO_DIR` para a pasta antiga, quando ela existir. Sem essa variavel, somente esse teste e ignorado. Os demais testes usam dados ficticios e nao acessam bancos reais.

O GitHub Actions valida o projeto no Windows com Python 3.12 e 3.14. A execucao com Python 3.12 tambem gera o pacote e disponibiliza ZIP e SHA-256 como artefatos do workflow.

## Dados e credenciais

Nao versione `.venv`, `saida`, `dist`, perfis reais, arquivos `.env`, CSVs de clientes, Parquets, planilhas, logs ou diagnosticos SQL. A `.gitignore` usa uma allowlist porque o ambiente de trabalho pode conter esses materiais. Ao criar novos arquivos de codigo na raiz, atualize essa lista conscientemente.

As senhas sao informadas pela interface ou por variaveis de ambiente e nao sao gravadas em perfis ou relatorios do painel. Logs de execucoes reais podem conter dados pessoais e devem permanecer fora do repositorio.

O banco de destino deve ter as tabelas e procedures exigidas pelo motor. O projeto nao executa alteracoes de estrutura automaticamente.
