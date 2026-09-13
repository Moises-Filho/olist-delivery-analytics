-- Databricks notebook source
-- MAGIC %md # P1 — Taxa de atraso por estado e evolução mensal
-- MAGIC Usa a agregada `agg_atraso_uf_mes` (já vem pronta do gold).
-- MAGIC `pedidos >= 50` filtra combinações de mês/estado com poucos
-- MAGIC pedidos, onde a taxa de atraso não é confiável.

-- COMMAND ----------

SELECT ano_mes, customer_state,
       pedidos, ROUND(taxa_atraso * 100, 1) AS pct_atraso
FROM olist_gold.mart.agg_atraso_uf_mes
WHERE pedidos >= 50
ORDER BY ano_mes, pct_atraso DESC;

-- COMMAND ----------
-- MAGIC %md # P2 — Atraso destrói a nota? (a pergunta central)
-- MAGIC Compara a nota média entre as faixas de atraso (no_prazo,
-- MAGIC 1_a_3_dias, etc). Só entra pedido entregue e com nota, senão
-- MAGIC a média fica distorcida por pedido sem avaliação.

-- COMMAND ----------

SELECT faixa_atraso,
       COUNT(DISTINCT order_id)   AS pedidos,
       ROUND(AVG(review_score),2) AS nota_media,
       ROUND(SUM(gmv), 2)         AS gmv
FROM olist_gold.mart.fct_order_items
WHERE order_status = 'delivered' AND review_score IS NOT NULL
GROUP BY faixa_atraso
ORDER BY nota_media DESC;

-- COMMAND ----------
-- MAGIC %md # P3 — Piores categorias em logística
-- MAGIC Usa a agregada `agg_performance_categoria`, que já exclui
-- MAGIC categorias com poucos itens. Aqui só ordena e pega o top 15
-- MAGIC com maior % de atraso.

-- COMMAND ----------

SELECT categoria_en, itens,
       ROUND(dias_entrega_medio, 1) AS dias_medio,
       ROUND(taxa_atraso * 100, 1)  AS pct_atraso,
       ROUND(nota_media, 2)         AS nota
FROM olist_gold.mart.agg_performance_categoria
ORDER BY pct_atraso DESC
LIMIT 15;

-- COMMAND ----------
-- MAGIC %md # P4 — GMV exposto a atraso
-- MAGIC Quanto do faturamento (gmv) está em pedidos que atrasaram,
-- MAGIC em valor absoluto e em percentual do total entregue.

-- COMMAND ----------

SELECT
  ROUND(SUM(CASE WHEN flag_atrasado THEN gmv ELSE 0 END), 2) AS gmv_atrasado,
  ROUND(SUM(gmv), 2)                                         AS gmv_total,
  ROUND(100.0 * SUM(CASE WHEN flag_atrasado THEN gmv ELSE 0 END)
        / SUM(gmv), 2)                                       AS pct_exposto
FROM olist_gold.mart.fct_order_items
WHERE order_status = 'delivered';

-- COMMAND ----------
-- MAGIC %md # P5 — Distância (proxy por UF) afeta o atraso?
-- MAGIC Não temos a distância real entre cliente e vendedor, então
-- MAGIC usamos "mesmo estado ou não" como um proxy simples pra isso.

-- COMMAND ----------

SELECT
  CASE WHEN c.customer_state = s.seller_state
       THEN 'mesma_uf' ELSE 'uf_diferente' END AS rota,
  COUNT(*)                                     AS itens,
  ROUND(AVG(f.dias_entrega), 1)                AS dias_medio,
  ROUND(AVG(CASE WHEN f.flag_atrasado THEN 1.0 ELSE 0.0 END)*100, 1) AS pct_atraso
FROM olist_gold.mart.fct_order_items f
JOIN olist_gold.mart.dim_customer c ON f.customer_sk = c.customer_sk
JOIN olist_gold.mart.dim_seller  s ON f.seller_sk  = s.seller_sk
WHERE f.order_status = 'delivered'
GROUP BY rota;

-- COMMAND ----------
-- MAGIC %md # P6 — Pior quartil de vendedores com volume relevante
-- MAGIC A agregada `agg_ranking_seller` já divide os vendedores em 4
-- MAGIC grupos (quartis) pela taxa de atraso, do pior (1) pro melhor (4),
-- MAGIC e já filtrou vendedores com poucas vendas. Aqui pegamos só o
-- MAGIC quartil 1 (os piores) e olhamos os 20 mais atrasados dele.

-- COMMAND ----------

SELECT seller_id, seller_state, itens,
       ROUND(gmv, 2)               AS gmv,
       ROUND(taxa_atraso * 100, 1) AS pct_atraso,
       ROUND(nota_media, 2)        AS nota
FROM olist_gold.mart.agg_ranking_seller
WHERE quartil_atraso = 1
ORDER BY pct_atraso DESC
LIMIT 20;
