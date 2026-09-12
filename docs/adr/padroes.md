# Padrões de desenvolvimento

## Nomenclatura de recursos Azure

| Recurso | Padrão | Exemplo |
|---|---|---|
| Resource Group | rg-{projeto}-{env} | rg-olist-dev |
| Storage Account | st{projeto}{env}{regiao} | stolistdevbr |
| Data Factory | adf-{projeto}-{env} | adf-olist-dev |
| Databricks | dbw-{projeto}-{env} | dbw-olist-dev |
| Key Vault | kv-{projeto}-{env} | kv-olist-dev |
| Access Connector | ac-{projeto}-{env} | ac-olist-dev |

Storage Account não aceita hífen nem maiúscula (restrição da Azure).

## Nomenclatura de dados

| Objeto | Padrão | Exemplo |
|---|---|---|
| Catálogo | olist_{camada} | olist_silver |
| Tabela bronze | nome da origem | orders |
| Tabela silver | nome da origem | orders |
| Tabela fato | fct_{processo} | fct_order_items |
| Tabela dimensão | dim_{entidade} | dim_customer |
| Tabela agregada | agg_{assunto} | agg_atraso_uf_mes |
| Coluna | snake_case | order_purchase_ts |
| Chave substituta | {entidade}_sk | customer_sk |
| Coluna técnica | _prefixo | _ingestion_ts |

## Caminhos no lake

abfss://{camada}@{conta}.dfs.core.windows.net/{dominio}/{tabela}/

Exemplo: abfss://bronze@stolistdevbr.dfs.core.windows.net/olist/orders/

## Particionamento

- bronze: _ingestion_date
- silver: sem partição (volume baixo)
- gold: order_year_month no fato

## Convenção de commits

Conventional Commits: feat, fix, docs, test, refactor, chore.
Exemplo: feat(silver): adiciona quarentena por regra de qualidade