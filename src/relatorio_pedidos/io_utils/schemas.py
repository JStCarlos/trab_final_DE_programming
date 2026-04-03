from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PEDIDOS_SCHEMA = StructType(
    [
        StructField("id_pedido", StringType(), nullable=True),
        StructField("produto", StringType(), nullable=True),
        StructField("valor_unitario", DoubleType(), nullable=True),
        StructField("quantidade", LongType(), nullable=True),
        StructField("data_criacao", TimestampType(), nullable=True),
        StructField("uf", StringType(), nullable=True),
        StructField("id_cliente", LongType(), nullable=True),
    ]
)

AVALIACAO_FRAUDE_SCHEMA = StructType(
    [
        StructField("fraude", BooleanType(), nullable=True),
        StructField("score", DoubleType(), nullable=True),
    ]
)

PAGAMENTOS_SCHEMA = StructType(
    [
        StructField("id_pedido", StringType(), nullable=True),
        StructField("forma_pagamento", StringType(), nullable=True),
        StructField("valor_pagamento", DoubleType(), nullable=True),
        StructField("status", BooleanType(), nullable=True),
        StructField("data_processamento", TimestampType(), nullable=True),
        StructField("avaliacao_fraude", AVALIACAO_FRAUDE_SCHEMA, nullable=True),
    ]
)

RELATORIO_SCHEMA = StructType(
    [
        StructField("id_pedido", StringType(), nullable=True),
        StructField("uf", StringType(), nullable=True),
        StructField("forma_pagamento", StringType(), nullable=True),
        StructField("valor_total", DoubleType(), nullable=True),
        StructField("data_pedido", TimestampType(), nullable=True),
    ]
)
