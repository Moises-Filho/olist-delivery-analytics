# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — modelo dimensional (star schema)
# MAGIC Grão do fato: **um item de um pedido**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md ## dim_date — gerada por código, não vem de fonte
# MAGIC
# MAGIC Não existe uma tabela de datas na origem, então criamos uma
# MAGIC linha para cada dia do período coberto pelos dados. Isso facilita
# MAGIC filtrar e agrupar por ano/mês/trimestre lá no BI.

# COMMAND ----------

datas = spark.sql("""
  SELECT explode(sequence(
      to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day
  )) AS data
""")

# datas das Black Fridays, usadas só pra marcar a flag_black_friday abaixo
FERIADOS = ["2016-11-25", "2017-11-24", "2018-11-23"]

dim_date = (datas
    # date_sk no formato yyyyMMdd (ex.: 20190301): é a chave usada
    # pra ligar essa dimensão com a tabela fato.
    .withColumn("date_sk", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("dia_semana", F.dayofweek("data"))
    .withColumn("nome_dia_semana", F.date_format("data", "EEEE"))
    # dayofweek: 1 = domingo, 7 = sábado
    .withColumn("flag_fim_semana", F.col("dia_semana").isin(1, 7))
    .withColumn("flag_black_friday",
                F.date_format("data", "yyyy-MM-dd").isin(FERIADOS))
    .select("date_sk", "data", "ano", "mes", "dia", "trimestre",
            "ano_mes", "dia_semana", "nome_dia_semana",
            "flag_fim_semana", "flag_black_friday"))

(dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_gold.mart.dim_date"))

# COMMAND ----------
# MAGIC %md ## dim_customer — com SCD Type 2
# MAGIC Na carga inicial toda linha é a versão vigente. A estrutura
# MAGIC já está preparada para versionar mudanças em cargas futuras.

# COMMAND ----------

src_customers = spark.table("olist_silver.clean.customers")

dim_customer = (src_customers
    # customer_sk é um hash de customer_id + customer_state: se o
    # estado do cliente mudar numa carga futura, isso gera uma nova
    # chave (uma nova "versão" do cliente), sem sobrescrever a antiga.
    .withColumn("customer_sk",
        F.sha2(F.concat_ws("||", "customer_id", "customer_state"), 256))
    # colunas de controle do SCD Type 2. Na carga inicial não existe
    # histórico ainda, então toda linha nasce "válida para sempre".
    .withColumn("valido_de", F.lit("1900-01-01").cast("date"))
    .withColumn("valido_ate", F.lit("9999-12-31").cast("date"))
    .withColumn("flag_atual", F.lit(True))
    .select("customer_sk", "customer_id", "customer_unique_id",
            "customer_zip", "customer_city", "customer_state",
            "valido_de", "valido_ate", "flag_atual"))

(dim_customer.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_gold.mart.dim_customer"))

# COMMAND ----------
# MAGIC %md ## dim_seller

# COMMAND ----------

src_sellers = spark.table("olist_silver.clean.sellers")

dim_seller = (src_sellers
    # aqui não precisa de SCD: seller_id sozinho já é uma chave estável
    .withColumn("seller_sk", F.sha2(F.col("seller_id"), 256))
    .select("seller_sk", "seller_id", "seller_zip",
            "seller_city", "seller_state"))

(dim_seller.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_gold.mart.dim_seller"))

# COMMAND ----------
# MAGIC %md ## dim_product

# COMMAND ----------

src_products = spark.table("olist_silver.clean.products")

dim_product = (src_products
    .withColumn("product_sk", F.sha2(F.col("product_id"), 256))
    # volume aproximado da caixa do produto, útil pra análises de frete
    .withColumn("volume_cm3",
        F.col("comprimento_cm") * F.col("altura_cm") * F.col("largura_cm"))
    # agrupa o peso em faixas, mais fácil de analisar do que o
    # peso exato em gramas
    .withColumn("faixa_peso",
        F.when(F.col("peso_g") < 500, "leve")
         .when(F.col("peso_g") < 2000, "medio")
         .when(F.col("peso_g").isNotNull(), "pesado")
         .otherwise("desconhecido"))
    .select("product_sk", "product_id", "categoria_pt", "categoria_en",
            "peso_g", "volume_cm3", "faixa_peso", "qtd_fotos"))

(dim_product.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_gold.mart.dim_product"))

# COMMAND ----------
# MAGIC %md ## fct_order_items
# MAGIC Junta item do pedido + pedido + review + as três dimensões,
# MAGIC e mantém só as chaves (os "sk") das dimensões — não os dados
# MAGIC completos, que já estão nas tabelas dim_*.

# COMMAND ----------

itens = spark.table("olist_silver.clean.order_items")
pedidos = spark.table("olist_silver.clean.orders")
reviews = spark.table("olist_silver.clean.order_reviews")

# só a versão vigente de cada cliente (SCD Type 2)
dim_customer_atual = spark.table("olist_gold.mart.dim_customer").filter("flag_atual")
dim_seller_completa = spark.table("olist_gold.mart.dim_seller")
dim_product_completa = spark.table("olist_gold.mart.dim_product")

fato = (itens.alias("i")
    # inner: um item de pedido sem o pedido correspondente não faz sentido
    .join(pedidos.alias("o"), "order_id", "inner")
    # left: nem todo pedido tem review, mas o item continua valendo
    .join(reviews.alias("r"), "order_id", "left")
    .join(dim_customer_atual.alias("dc"),
          F.col("o.customer_id") == F.col("dc.customer_id"), "left")
    .join(dim_seller_completa.alias("ds"),
          F.col("i.seller_id") == F.col("ds.seller_id"), "left")
    .join(dim_product_completa.alias("dp"),
          F.col("i.product_id") == F.col("dp.product_id"), "left")
    .select(
        # chave do fato: cada linha é um item dentro de um pedido
        F.sha2(F.concat_ws("||", F.col("i.order_id"),
                           F.col("i.order_item_id")), 256).alias("order_item_sk"),
        F.col("i.order_id"),                       # dimensão degenerada
        F.col("i.order_item_id"),
        F.col("dc.customer_sk"),
        F.col("ds.seller_sk"),
        F.col("dp.product_sk"),
        F.date_format(F.col("o.purchase_ts"), "yyyyMMdd")
            .cast("int").alias("date_sk"),
        F.col("o.order_status"),
        F.col("i.price"),
        F.col("i.freight_value"),
        F.col("i.gmv"),
        F.col("o.dias_entrega"),
        F.col("o.dias_atraso"),
        F.col("o.flag_atrasado"),
        F.col("o.faixa_atraso"),
        F.col("r.review_score"),
        # ano_mes vira coluna de partição lá embaixo, pra acelerar
        # consultas que filtram por período
        F.date_format(F.col("o.purchase_ts"), "yyyy-MM").alias("ano_mes"),
    ))

(fato.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano_mes")
    .saveAsTable("olist_gold.mart.fct_order_items"))

# COMMAND ----------
# MAGIC %md ## Otimização física
# MAGIC ZORDER organiza os arquivos fisicamente por essas colunas, o
# MAGIC que acelera consultas que filtram/agrupam por cliente ou data.

# COMMAND ----------

spark.sql("""
  OPTIMIZE olist_gold.mart.fct_order_items
  ZORDER BY (customer_sk, date_sk)
""")

# COMMAND ----------
# MAGIC %md ## Agregadas para o BI
# MAGIC Tabelas já resumidas, pra o BI não precisar reprocessar a fato
# MAGIC inteira toda vez que alguém abre um dashboard.
# MAGIC Em todas, `WHERE order_status = 'delivered'` porque só faz
# MAGIC sentido medir atraso/nota de pedidos que chegaram ao cliente.

# COMMAND ----------

# atraso e faturamento por estado do cliente, mês a mês
spark.sql("""
CREATE OR REPLACE TABLE olist_gold.mart.agg_atraso_uf_mes AS
SELECT
  d.ano_mes,
  c.customer_state,
  COUNT(DISTINCT f.order_id)                      AS pedidos,
  SUM(f.gmv)                                      AS gmv,
  AVG(CASE WHEN f.flag_atrasado THEN 1.0 ELSE 0.0 END) AS taxa_atraso,
  AVG(f.dias_entrega)                             AS dias_entrega_medio,
  AVG(f.review_score)                             AS nota_media
FROM olist_gold.mart.fct_order_items f
JOIN olist_gold.mart.dim_date d     ON f.date_sk = d.date_sk
JOIN olist_gold.mart.dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.order_status = 'delivered'
GROUP BY d.ano_mes, c.customer_state
""")

# performance por categoria de produto e faixa de peso.
# HAVING COUNT(*) >= 30 evita mostrar categorias com poucos itens,
# onde a média não é confiável.
spark.sql("""
CREATE OR REPLACE TABLE olist_gold.mart.agg_performance_categoria AS
SELECT
  p.categoria_en,
  p.faixa_peso,
  COUNT(*)                                        AS itens,
  SUM(f.gmv)                                      AS gmv,
  AVG(f.dias_entrega)                             AS dias_entrega_medio,
  AVG(CASE WHEN f.flag_atrasado THEN 1.0 ELSE 0.0 END) AS taxa_atraso,
  AVG(f.review_score)                             AS nota_media
FROM olist_gold.mart.fct_order_items f
JOIN olist_gold.mart.dim_product p ON f.product_sk = p.product_sk
WHERE f.order_status = 'delivered'
GROUP BY p.categoria_en, p.faixa_peso
HAVING COUNT(*) >= 30
""")

# ranking de vendedores por taxa de atraso, divididos em quartis
# (ORDER BY taxa_atraso DESC: quartil_atraso = 1 é o grupo com MAIOR
# taxa de atraso, os piores vendedores).
# HAVING COUNT(*) >= 50 evita rankear vendedor com poucas vendas.
spark.sql("""
CREATE OR REPLACE TABLE olist_gold.mart.agg_ranking_seller AS
WITH perf AS (
  SELECT
    s.seller_id,
    s.seller_state,
    COUNT(*)                                        AS itens,
    SUM(f.gmv)                                      AS gmv,
    AVG(CASE WHEN f.flag_atrasado THEN 1.0 ELSE 0.0 END) AS taxa_atraso,
    AVG(f.review_score)                             AS nota_media
  FROM olist_gold.mart.fct_order_items f
  JOIN olist_gold.mart.dim_seller s ON f.seller_sk = s.seller_sk
  WHERE f.order_status = 'delivered'
  GROUP BY s.seller_id, s.seller_state
  HAVING COUNT(*) >= 50
)
SELECT *, NTILE(4) OVER (ORDER BY taxa_atraso DESC) AS quartil_atraso
FROM perf
""")

print("gold concluído")
