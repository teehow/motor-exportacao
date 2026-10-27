# Arquivos de dados

Esta pasta nao contem dados reais. Os CSVs em `exemplos` usam valores ficticios e nao sao selecionados automaticamente pelo painel.

Prepare seus CSVs e selecione os caminhos na aba Filtros e arquivos. Deixe o campo vazio quando nao quiser usar um filtro opcional.

| Arquivo de exemplo | Uso |
| --- | --- |
| projetos.csv | project.name, nao project.id |
| pendencias.csv | pendency.pendency_id, nao pendency.id |
| contratadas.csv | company.cnpj, preservando zeros iniciais |
| project_company_account.csv | project_company_account.id; nessa selecao ignora PCA.deleted |
| contratoNumero.csv | project_id;contrato_numero;project.name;contratada (quarta coluna opcional) |
| grupoAreas.csv | contrato_numero;novo_grupo_area;nova_area, sem cabecalho |
| grupos.csv | contrato_numero;novo_grupo, sem cabecalho |

`grupoAreas.csv` e usado em contratos e `grupos.csv` em riscos. Os scripts existentes exigem esses arquivos. Sem correspondencia, os valores originais sao mantidos; nao use dados ficticios para uma migracao real sem antes revisar os mapeamentos.

Use UTF-8 e separador ponto e virgula nos mapeamentos de grupos e areas. Os filtros e o de/para de contratos tambem reconhecem virgula e tabulacao.
