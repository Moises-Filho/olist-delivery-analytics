# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — limpeza, tipagem, deduplicação e quarentena

# COMMAND ----------

dbutils.widgets.text("batch_id", "")
BATCH_ID = dbutils.widgets.get("batch_id")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md ## Funções auxiliares
# MAGIC
# MAGIC Usadas em todas as tabelas abaixo, pra não repetir o mesmo código
# MAGIC várias vezes.

# COMMAND ----------

def aplica_regras(df, regras):
    """Separa o df em (válidos, inválidos) a partir de um dicionário
    de regras {nome_da_regra: condição}.

    Uma linha só é válida se passar em TODAS as regras. As linhas
    inválidas ganham a coluna _motivo_rejeicao dizendo quais regras
    elas não passaram.
    """
    # Junta todas as condições em uma só com "&" (E lógico).
    cond_ok = None
    for condicao in regras.values():
        if cond_ok is None:
            cond_ok = condicao
        else:
            cond_ok = cond_ok & condicao

    validos = df.filter(cond_ok)

    # Para cada regra que a linha não passou, guarda o nome dela.
    # concat_ws junta os nomes com ";" e ignora os nulos (as regras
    # que a linha passou).
    nomes_das_regras_que_falharam = [
        F.when(~condicao, F.lit(nome)) for nome, condicao in regras.items()
    ]
    motivo = F.concat_ws(";", *nomes_das_regras_que_falharam)

    invalidos = (df.filter(~cond_ok)
                    .withColumn("_motivo_rejeicao", motivo)
                    .withColumn("_quarantine_ts", F.current_timestamp()))

    return validos, invalidos


def grava_quarentena(df, tabela):
    """Grava as linhas rejeitadas numa tabela de quarentena, pra dar
    pra investigar depois. Se não tiver nenhuma linha rejeitada, não
    escreve nada (evita gravar tabela vazia sem necessidade).
    """
    if df.count() > 0:
        (df.write.format("delta").mode("append")
            .saveAsTable(f"olist_silver.quarantine.{tabela}"))


def normaliza_texto(col):
    """Deixa um texto em minúsculo, sem espaços nas pontas e sem
    espaços duplicados no meio. Útil pra cidade, categoria etc.
    """
    return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))


# formato de data usado em todas as colunas de timestamp dos csvs
FORMATO_TIMESTAMP = "yyyy-MM-dd HH:mm:ss"

# COMMAND ----------
# MAGIC %md ## customers

# COMMAND ----------

bronze_customers = spark.table("olist_bronze.raw.customers")

df = bronze_customers.select(
    F.col("customer_id"),
    F.col("customer_unique_id"),
    # cep sempre com 5 dígitos, preenchendo com zero à esquerda se faltar
    F.lpad(F.col("customer_zip_code_prefix"), 5, "0").alias("customer_zip"),
    normaliza_texto(F.col("customer_city")).alias("customer_city"),
    F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
    F.col("_batch_id"),
)

regras = {
    "customer_id_nao_nulo": F.col("customer_id").isNotNull(),
    "uf_com_2_letras": F.length(F.col("customer_state")) == 2,
}
validos, invalidos = aplica_regras(df, regras)
grava_quarentena(invalidos, "customers")

# pode existir o mesmo customer_id vindo de mais de um batch: fica
# só a versão do batch mais recente.
janela = Window.partitionBy("customer_id").orderBy(F.col("_batch_id").desc())
(validos.withColumn("_rn", F.row_number().over(janela))
    .filter("_rn = 1")
    .drop("_rn")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("olist_silver.clean.customers"))

# COMMAND ----------
# MAGIC %md ## sellers

# COMMAND ----------

bronze_sellers = spark.table("olist_bronze.raw.sellers")

df = bronze_sellers.select(
    F.col("seller_id"),
    F.lpad(F.col("seller_zip_code_prefix"), 5, "0").alias("seller_zip"),
    normaliza_texto(F.col("seller_city")).alias("seller_city"),
    F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
)

regras = {
    "seller_id_nao_nulo": F.col("seller_id").isNotNull(),
    "uf_com_2_letras": F.length(F.col("seller_state")) == 2,
}
validos, invalidos = aplica_regras(df, regras)
grava_quarentena(invalidos, "sellers")

(validos.dropDuplicates(["seller_id"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("olist_silver.clean.sellers"))

# COMMAND ----------
# MAGIC %md ## products (com tradução de categoria)

# COMMAND ----------

bronze_products = spark.table("olist_bronze.raw.products")
bronze_traducao = spark.table("olist_bronze.raw.category_translation")

# junta cada produto com a tradução (em inglês) da sua categoria.
# "left" porque queremos manter o produto mesmo se não achar tradução.
df = (bronze_products.alias("p")
      .join(bronze_traducao.alias("t"),
            F.col("p.product_category_name") == F.col("t.product_category_name"),
            "left")
      .select(
          F.col("p.product_id"),
          F.coalesce(normaliza_texto(F.col("p.product_category_name")),
                     F.lit("sem_categoria")).alias("categoria_pt"),
          F.coalesce(normaliza_texto(F.col("t.product_category_name_english")),
                     F.lit("unknown")).alias("categoria_en"),
          F.col("p.product_weight_g").cast("double").alias("peso_g"),
          F.col("p.product_length_cm").cast("double").alias("comprimento_cm"),
          F.col("p.product_height_cm").cast("double").alias("altura_cm"),
          F.col("p.product_width_cm").cast("double").alias("largura_cm"),
          F.col("p.product_photos_qty").cast("int").alias("qtd_fotos"),
      ))

regras = {
    "product_id_nao_nulo": F.col("product_id").isNotNull(),
    "peso_nao_negativo": F.coalesce(F.col("peso_g"), F.lit(0)) >= 0,
}
validos, invalidos = aplica_regras(df, regras)
grava_quarentena(invalidos, "products")

(validos.dropDuplicates(["product_id"])
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("olist_silver.clean.products"))

# COMMAND ----------
# MAGIC %md ## orders — aqui nascem as métricas de atraso

# COMMAND ----------

bronze_orders = spark.table("olist_bronze.raw.orders")

df = bronze_orders.select(
    F.col("order_id"),
    F.col("customer_id"),
    F.lower(F.trim(F.col("order_status"))).alias("order_status"),
    F.to_timestamp("order_purchase_timestamp", FORMATO_TIMESTAMP).alias("purchase_ts"),
    F.to_timestamp("order_approved_at", FORMATO_TIMESTAMP).alias("approved_ts"),
    F.to_timestamp("order_delivered_carrier_date", FORMATO_TIMESTAMP).alias("carrier_ts"),
    F.to_timestamp("order_delivered_customer_date", FORMATO_TIMESTAMP).alias("delivered_ts"),
    F.to_timestamp("order_estimated_delivery_date", FORMATO_TIMESTAMP).alias("estimated_ts"),
)

df = (df
      # quantos dias entre a compra e a entrega
      .withColumn("dias_entrega", F.datediff("delivered_ts", "purchase_ts"))
      # positivo = entregou depois do prazo estimado (atrasado)
      .withColumn("dias_atraso", F.datediff("delivered_ts", "estimated_ts"))
      .withColumn("flag_atrasado",
                  F.when(F.col("dias_atraso") > 0, True)
                  .when(F.col("dias_atraso") <= 0, False)
                  # dias_atraso nulo = pedido ainda não entregue, não dá pra saber
                  .otherwise(None))
      .withColumn("faixa_atraso",
        F.when(F.col("dias_atraso").isNull(), "nao_entregue")
        .when(F.col("dias_atraso") <= 0, "no_prazo")
        .when(F.col("dias_atraso") <= 3, "1_a_3_dias")
        .when(F.col("dias_atraso") <= 7, "4_a_7_dias")
        .otherwise("8_ou_mais")))

regras = {
    "order_id_nao_nulo": F.col("order_id").isNotNull(),
    "customer_id_nao_nulo": F.col("customer_id").isNotNull(),
    "purchase_ts_nao_nulo": F.col("purchase_ts").isNotNull(),
    # não faz sentido ter sido entregue antes de ter sido comprado
    "entrega_depois_da_compra":
        F.col("delivered_ts").isNull() |
        (F.col("delivered_ts") >= F.col("purchase_ts")),
}
validos, invalidos = aplica_regras(df, regras)
grava_quarentena(invalidos, "orders")

(validos.dropDuplicates(["order_id"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("olist_silver.clean.orders"))

# COMMAND ----------
# MAGIC %md ## order_items

# COMMAND ----------

bronze_order_items = spark.table("olist_bronze.raw.order_items")

df = bronze_order_items.select(
    F.col("order_id"),
    F.col("order_item_id").cast("int").alias("order_item_id"),
    F.col("product_id"),
    F.col("seller_id"),
    F.col("price").cast("double").alias("price"),
    F.col("freight_value").cast("double").alias("freight_value"),
    # gmv = valor do produto + frete, pra facilitar as análises depois
).withColumn("gmv", F.col("price") + F.col("freight_value"))

regras = {
    "order_id_nao_nulo": F.col("order_id").isNotNull(),
    "preco_positivo": F.col("price") > 0,
    "frete_nao_negativo": F.col("freight_value") >= 0,
}
validos, invalidos = aplica_regras(df, regras)
grava_quarentena(invalidos, "order_items")

(validos.dropDuplicates(["order_id", "order_item_id"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("olist_silver.clean.order_items"))

# COMMAND ----------
# MAGIC %md ## order_reviews — uma nota por pedido

# COMMAND ----------

bronze_order_reviews = spark.table("olist_bronze.raw.order_reviews")

df = bronze_order_reviews.select(
    F.col("review_id"),
    F.col("order_id"),
    F.col("review_score").cast("int").alias("review_score"),
    F.to_timestamp("review_creation_date", FORMATO_TIMESTAMP).alias("review_ts"),
)

regras = {
    "order_id_nao_nulo": F.col("order_id").isNotNull(),
    "nota_entre_1_e_5": F.col("review_score").between(1, 5),
}
validos, invalidos = aplica_regras(df, regras)
grava_quarentena(invalidos, "order_reviews")

# alguns pedidos têm mais de uma review: fica a mais recente
janela = Window.partitionBy("order_id").orderBy(F.col("review_ts").desc_nulls_last())
(validos.withColumn("_rn", F.row_number().over(janela))
    .filter("_rn = 1")
    .drop("_rn")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("olist_silver.clean.order_reviews"))

# COMMAND ----------
# MAGIC %md ## Conferência final
# MAGIC
# MAGIC Só pra ver quantas linhas cada tabela silver ficou com,
# MAGIC ajuda a notar rapidinho se algo deu muito errado.

# COMMAND ----------

for tabela in ["customers", "sellers", "products", "orders",
            "order_items", "order_reviews"]:
    total_linhas = spark.table(f"olist_silver.clean.{tabela}").count()
    print(f"silver.{tabela}: {total_linhas}")
