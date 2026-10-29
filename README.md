# lago-dados

Esse repositório é um projeto de estudos de data engineering no Databricks: um lakehouse com arquitetura de medalhas (bronze, silver e, mais pra frente, gold) montado em cima de um sistema de fidelidade, o "loyalty system" do TeoMeWhy. A ideia é simular um cenário real de dados que mudam com o tempo, com ingestão incremental via CDC na bronze e a silver se mantendo em sincronia consumindo o Change Data Feed do Delta Lake, tudo sem recarregar a base inteira a cada execução.

A fonte é o dataset [teomewhy-loyalty-system](https://www.kaggle.com/datasets/teocalvo/teomewhy-loyalty-system) do Kaggle, com clientes, transações e os produtos de cada transação. O fluxo em uma linha:

```
Kaggle (CSV) → Volume (Unity Catalog) → Bronze (Delta + CDF) → Silver (Delta + CDF) → Gold (ainda reservado)
```

## Como o projeto está organizado

```
lago-dados/
├── .github/workflows/main.yml     # CI que sincroniza os jobs no Databricks
├── README.md
└── src/
    ├── bronze/
    │   ├── ingestao               # notebook de ingestão bronze (parametrizado)
    │   ├── Consumo                # notebook de apoio: validações e drops de teste
    │   ├── customers.json         # schemas Spark das tabelas
    │   ├── transactions.json
    │   └── transactions_product.json
    ├── silver/
    │   ├── ingestao               # notebook de ingestão silver (parametrizado)
    │   ├── cliente.sql            # queries de transformação bronze → silver
    │   ├── transacao.sql
    │   ├── produtos.sql
    │   └── transacao_produto.sql
    ├── silver_fs/                 # vazio por enquanto
    ├── gold/                      # reservado pra camada analítica
    ├── lib/
    │   ├── utils.py               # helpers de schema, query e checagem de tabela
    │   └── ingestors.py           # as classes de ingestão (full load, CDC e CDF)
    └── workflows/
        ├── main.py                # aplica os jobs no Databricks via API REST
        ├── upsell.json            # settings do job "upsell"
        └── requirements.txt       # dependências do main.py (roda fora do Databricks)
```

## A fonte de dados

São três CSVs separados por `;` (com cabeçalho), numa versão fixa do dataset (4540):

* `clientes.csv` alimenta a bronze `customers`
* `transacoes.csv` alimenta a bronze `transactions`
* `transacao_produto.csv` alimenta a bronze `transactions_product`

Um detalhe que evita dor de cabeça: o schema de cada tabela é declarado num JSON em `src/bronze/` e forçado na leitura, então a tipagem nunca depende do que o Spark resolve inferir do arquivo.

## Camada bronze

O notebook `src/bronze/ingestao` recebe tudo por widgets (`tablename`, `id_field`, `timestamp_field` e `database`) e roda uma vez por tabela. O comportamento muda conforme o estado dela.

Se a tabela ainda não existe em `workspace.bronze`, é um full load: qualquer checkpoint antigo é removido, os CSVs de `/Volumes/workspace/upsell/full_load/<tabela>/` são lidos com o schema do JSON e gravados como tabela Delta com o Change Data Feed habilitado.

Se a tabela já existe, entra o modo incremental. O notebook baixa a versão atual do dataset no Kaggle com o `kagglehub` e joga o arquivo em `/Volumes/workspace/upsell/cdc/kaggle/<tabela>/`, com um sufixo de timestamp no nome. Um stream com Auto Loader (`cloudFiles`) lê esses arquivos e, dentro do `foreachBatch`, aplica um MERGE na tabela Delta: deduplica pelo `id_field` mantendo o registro com o `timestamp_field` mais recente (janela de `row_number`) e executa `whenMatchedUpdateAll` + `whenNotMatchedInsertAll`. Depois que o batch processa, os arquivos são removidos da pasta, então a próxima execução só enxerga o que é novo.

O notebook `Consumo`, na mesma pasta, é o caderno de rascunho do projeto: consultas de validação, um olhar no CDF com `table_changes()` e as células de `DROP TABLE` pra zerar o ambiente quando algum teste precisa recomeçar do zero.

## Camada silver

Mesma filosofia, notebook `src/silver/ingestao` com os widgets `tablename` e `id_field`. A diferença é que aqui a transformação mora fora do código, em arquivos `.sql`:

* `cliente.sql` parte de `bronze.customers` e renomeia `qtdePontos` pra `nrPontosCliente` e `flEmail` pra `flEmailCliente`
* `transacao.sql` parte de `bronze.transactions`, com `DtCriacao` virando `dtTransacao` e `QtdePontos` virando `nrPontosTransacao`
* `transacao_produto.sql` parte de `bronze.transactions_product`, com `QtdeProduto` virando `nrQuantidadeProduto`
* `produtos.sql` é o diferente: não consulta bronze nenhuma, gera um catálogo com os 16 produtos via `sequence` + `case`

Na primeira execução é full load de novo, e a silver nasce como Delta com CDF habilitado. A partir daí, um stream lê o Change Data Feed da tabela bronze de origem (`readChangeFeed = true`), que entrega os eventos com `_change_type`, `_commit_version` e `_commit_timestamp`. Batch a batch: cria uma view temporária, pega o registro mais recente por `id_field` (descartando o `update_preimage`) com `QUALIFY ROW_NUMBER()`, aplica a query de transformação e faz o MERGE na silver, com cada tipo de evento tratado: `delete` apaga, `update_postimage` atualiza ou insere, `insert` insere.

O checkpoint desse stream fica em `/Volumes/workspace/upsell/cdc/silver_<tabela>_checkpoint/` e é removido quando a tabela é recriada do zero.

## A lib (`src/lib`)

`utils.py` é o conjunto de helpers: `import_query` lê um `.sql`, `table_exists` checa se a tabela já existe (é ele quem decide entre full load e incremental), `import_schema` carrega o schema do JSON, e os demais (`extract_from`, `add_generic_from`, `add_fields`, `format_query_cdf`) fazem a cirurgia na query SQL: injetam as colunas do CDF no SELECT e trocam a tabela do `FROM` por um alias, pra que a mesma query sirva tanto pro full load quanto pro incremental.

`ingestors.py` concentra a ingestão em três classes que herdam uma da outra:

* `Ingestor` cuida do full load: lê os CSVs com schema fixo e grava a tabela Delta com CDF habilitado
* `IngestorCDC` herda dele e adiciona o incremental via Auto Loader (stream `cloudFiles` + `foreachBatch` + MERGE com dedup por timestamp), com checkpoint em `/Volumes/workspace/<schema>/cdc/checkpoint_<tabela>/`
* `IngestorCDF` herda de `IngestorCDC` e faz a ponte bronze → silver: lê o CDF da bronze, aplica a transformação SQL e faz o MERGE na silver tratando insert, update e delete

## Onde as coisas ficam

Tudo vive no catálogo `workspace`, schema `upsell`:

* `/Volumes/workspace/upsell/full_load/<tabela>/` – arquivos da carga inicial
* `/Volumes/workspace/upsell/cdc/kaggle/<tabela>/` – CDCs baixados do Kaggle
* `/Volumes/workspace/upsell/cdc/kaggle_cache/` – cache do `kagglehub`
* `/Volumes/workspace/upsell/cdc/silver_<tabela>_checkpoint/` – checkpoints dos streams silver
* `/Volumes/workspace/<schema>/cdc/checkpoint_<tabela>/` – checkpoints dos streams bronze

## Orquestração e CI

O job `upsell` (definido em `src/workflows/upsell.json`) é um job multi-tarefa do Databricks que roda todo dia às 09:00:39, horário de São Paulo, com o código vindo direto do Git (branch `main`). O DAG é curto e direto, cada silver depende da bronze correspondente:

```
bronze_customers ─────────► silver_cliente
bronze_transactions ─────► silver_transacao
bronze_transactions_product ─► silver_produtos
                            └► silver_transacao_produto
```

Cada tarefa executa o notebook `ingestao` da respectiva camada com os parâmetros da tabela.

Pra não precisar atualizar nada na mão, o `main.py` em `src/workflows/` percorre os `*.json` da pasta e aplica cada um via `POST /api/2.1/jobs/reset` da API do Databricks. As credenciais (`DATABRICKS_HOST` e `DATABRICKS_TOKEN`) vêm de um `.env` local, ignorado pelo git, ou dos secrets no CI. E o GitHub Actions roda esse sync automaticamente a cada push na `main`, então a definição do job no workspace sempre reflete o que está no repositório.

## O que o projeto produz

* Bronze: `workspace.bronze.customers`, `workspace.bronze.transactions` e `workspace.bronze.transactions_product`
* Silver: `workspace.silver.cliente`, `workspace.silver.transacao`, `workspace.silver.produtos` e `workspace.silver.transacao_produto`
* Gold: ainda não existe, a ideia é usá-lo pra análises e modelagem (recomendação de produtos / upsell)

## Pré-requisitos

* um workspace Databricks (o projeto foi desenvolvido em compute serverless)
* o catálogo `workspace` com os schemas `bronze` e `silver` e o volume `upsell` criados
* credenciais do Kaggle configuradas, pra o `kagglehub` conseguir baixar o dataset
* se quiser o CI funcionando, os secrets `DATABRICKS_HOST` e `DATABRICKS_TOKEN` no repositório

## Como rodar

No dia a dia, o push pra `main` já resolve: o Actions sincroniza o job e ele roda sozinho no horário agendado. Se quiser forçar uma execução, dá pra rodar o job `upsell` pela UI do Databricks. E pra testar uma tabela isolada, basta rodar o notebook de ingestão passando os widgets, por exemplo `tablename = customers`, `id_field = idCliente`, `timestamp_field = DtAtualizacao` e `database = bronze`.
