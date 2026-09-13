"""Schemas parat transformar na camada silver.

Nao usamos inferSchema em produção pois o Spark faria uma passagem
extra sobre os dados e o tipo inferido pode mudar entre execuções
conforme o conteúdo do arquivo.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)

# True = permite valores nulos (NULL)
# False = não permite valores nulos

# tabela de pedidos, uma linha por pedido
ORDERS = StructType([
    StructField("order_id", StringType(), False),  # id do pedido, chave primaria
    StructField("customer_id", StringType(), False),  # id do cliente que fez o pedido
    StructField("order_status", StringType(), True),  # status: delivered, shipped, canceled etc
    StructField("order_purchase_timestamp", TimestampType(), True),  # quando o pedido foi feito
    StructField("order_approved_at", TimestampType(), True),  # quando o pagamento foi aprovado
    StructField("order_delivered_carrier_date", TimestampType(), True),  # quando saiu pra transportadora
    StructField("order_delivered_customer_date", TimestampType(), True),  # quando chegou no cliente
    StructField("order_estimated_delivery_date", TimestampType(), True),  # data estimada de entrega
])

# itens de cada pedido, um pedido pode ter varios itens
ORDER_ITEMS = StructType([
    StructField("order_id", StringType(), False),  # liga com a tabela ORDERS
    StructField("order_item_id", IntegerType(), False),  # numero do item dentro do pedido (1, 2, 3...)
    StructField("product_id", StringType(), True),  # id do produto comprado
    StructField("seller_id", StringType(), True),  # id do vendedor desse item
    StructField("shipping_limit_date", TimestampType(), True),  # prazo limite pra despachar
    StructField("price", DoubleType(), True),  # preco do produto
    StructField("freight_value", DoubleType(), True),  # valor do frete
])

# dados dos clientes
CUSTOMERS = StructType([
    StructField("customer_id", StringType(), False),  # id usado nos pedidos (muda a cada pedido)
    StructField("customer_unique_id", StringType(), True),  # id fixo do cliente, nao muda
    StructField("customer_zip_code_prefix", StringType(), True),  # prefixo do cep
    StructField("customer_city", StringType(), True),  # cidade do cliente
    StructField("customer_state", StringType(), True),  # estado (sigla, ex: SP)
])

# dados dos vendedores
SELLERS = StructType([
    StructField("seller_id", StringType(), False),  # id do vendedor, chave primaria
    StructField("seller_zip_code_prefix", StringType(), True),  # prefixo do cep
    StructField("seller_city", StringType(), True),  # cidade do vendedor
    StructField("seller_state", StringType(), True),  # estado
])

# dados dos produtos
PRODUCTS = StructType([
    StructField("product_id", StringType(), False),  # id do produto, chave primaria
    StructField("product_category_name", StringType(), True),  # categoria do produto
    StructField("product_name_lenght", IntegerType(), True),  # tamanho do nome
    StructField("product_description_lenght", IntegerType(), True),  # tamanho da descricao
    StructField("product_photos_qty", IntegerType(), True),  # quantidade de fotos
    StructField("product_weight_g", DoubleType(), True),  # peso em gramas
    StructField("product_length_cm", DoubleType(), True),  # comprimento em cm
    StructField("product_height_cm", DoubleType(), True),  # altura em cm
    StructField("product_width_cm", DoubleType(), True),  # largura em cm
])

# avaliacoes que os clientes deixam depois do pedido
REVIEWS = StructType([
    StructField("review_id", StringType(), False),  # id da avaliacao
    StructField("order_id", StringType(), False),  # pedido que foi avaliado
    StructField("review_score", IntegerType(), True),  # nota de 1 a 5
    StructField("review_comment_title", StringType(), True),  # titulo do comentario
    StructField("review_comment_message", StringType(), True),  # texto do comentario
    StructField("review_creation_date", TimestampType(), True),  # quando a avaliacao foi criada
    StructField("review_answer_timestamp", TimestampType(), True),  # quando o cliente respondeu
])

# dicionario pra buscar o schema certo pelo nome da tabela
SCHEMAS = {
    "orders": ORDERS,
    "order_items": ORDER_ITEMS,
    "customers": CUSTOMERS,
    "sellers": SELLERS,
    "products": PRODUCTS,
    "order_reviews": REVIEWS,
}

# chave de negócio usada na deduplicação
# (colunas que juntas identificam uma linha unica de verdade)
CHAVES = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],  # sozinho order_id repete
    "customers": ["customer_id"],
    "sellers": ["seller_id"],
    "products": ["product_id"],
    "order_reviews": ["review_id"],
}