# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — ingestão bruta
# MAGIC Lê os CSVs e grava em Delta **sem transformação**,
# MAGIC acrescentando apenas colunas de auditoria.

# COMMAND ----------

# Parâmetros do notebook 
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("landing_path", "/Volumes/olist_bronze/raw/landing")

# Se ninguém passar um batch_id, gera um id curto e único (12 caracteres)
# só pra conseguir rastrear essa execução depois.
# LANDING é a pasta onde os CSVs originais estão salvos.
BATCH_ID = dbutils.widgets.get("batch_id") or __import__("uuid").uuid4().hex[:12]
LANDING = dbutils.widgets.get("landing_path")

# COMMAND ----------

from pyspark.sql import functions as F

# Mapa "nome da tabela no bronze" -> "nome do arquivo csv de origem".
ARQUIVOS = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# COMMAND ----------

# Para cada arquivo do mapa acima: lê o csv, adiciona colunas de auditoria
# e grava como uma tabela Delta na camada bronze.
for tabela, arquivo in ARQUIVOS.items():

    df = (spark.read
        .option("header", True)
        # inferSchema desligado de propósito: no bronze tudo fica como
        # string. Quem decide o tipo certo de cada coluna é a camada
        # seguinte (silver), não a ingestão bruta.
        .option("inferSchema", False)
        # os textos das avaliações (reviews) podem ter quebra de linha
        # dentro do próprio campo, então precisamos avisar o Spark que
        # um "registro" pode ocupar mais de uma linha do arquivo.
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"{LANDING}/{arquivo}"))

    # Colunas de auditoria: não vêm do csv, servem pra rastrear de onde
    # e quando cada linha chegou (útil pra debugar e pra reprocessar).
    (df.withColumn("_ingestion_ts", F.current_timestamp())    # quando foi lido
       .withColumn("_ingestion_date", F.current_date())        # data (usada como partição)
       .withColumn("_source_file", F.lit(arquivo))             # de qual csv veio
       .withColumn("_batch_id", F.lit(BATCH_ID))                # de qual execução veio
       .write.format("delta")
       # overwrite: o bronze sempre reflete a última carga completa do
       # csv, não fica acumulando execuções antigas.
       .mode("overwrite")
       # permite que o schema da tabela mude entre execuções (ex.: se o
       # csv ganhar uma coluna nova), já que aqui tudo é string mesmo.
       .option("overwriteSchema", "true")
       .partitionBy("_ingestion_date")
       .saveAsTable(f"olist_bronze.raw.{tabela}"))

    # Log pra acompanhar o andamento da carga no console/log do job.
    print(f"{tabela}: {df.count()} linhas")

# COMMAND ----------

# Devolve o batch_id pra quem chamou esse notebook (ex.: um job orquestrador),
# assim as próximas etapas do pipeline sabem qual foi o lote processado aqui.
dbutils.notebook.exit(BATCH_ID)
