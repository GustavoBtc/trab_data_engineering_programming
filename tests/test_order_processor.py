
import pytest

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    IntegerType, BooleanType
)

from business.order_processor import OrderProcessor


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TesteOrderProcessor")
        .getOrCreate()
    )

    yield session
    session.stop()


def test_processamento_pedidos(spark):

    # Schema explicito dos pedidos
    schema_pedidos = StructType([
        StructField("ID_PEDIDO", StringType()),
        StructField("UF", StringType()),
        StructField("DATA_CRIACAO", StringType()),
        StructField("VALOR_UNITARIO", DoubleType()),
        StructField("QUANTIDADE", IntegerType())
    ])

    dados_pedidos = [
        ("001", "SP", "2025-03-10", 100.0, 2),
        ("002", "RJ", "2025-04-15", 50.0, 3),
        ("003", "MG", "2024-05-20", 80.0, 1),
        ("004", "BA", "2025-06-01", 70.0, 1)
    ]

    df_pedidos = spark.createDataFrame(
        dados_pedidos,
        schema=schema_pedidos
    )

    # Schema explicito dos pagamentos
    schema_fraude = StructType([
        StructField("fraude", BooleanType()),
        StructField("score", DoubleType())
    ])

    schema_pagamentos = StructType([
        StructField("id_pedido", StringType()),
        StructField("forma_pagamento", StringType()),
        StructField("status", BooleanType()),
        StructField("avaliacao_fraude", schema_fraude)
    ])

    dados_pagamentos = [
        ("001", "PIX", False, (False, 0.1)),
        ("002", "BOLETO", True, (False, 0.2)),
        ("003", "PIX", False, (False, 0.3)),
        ("004", "PIX", False, (True, 0.9))
    ]

    df_pagamentos = spark.createDataFrame(
        dados_pagamentos,
        schema=schema_pagamentos
    )

    # Executa a logica de negocio
    processor = OrderProcessor()

    resultado = processor.process(
        df_pedidos,
        df_pagamentos
    )

    registros = resultado.collect()

    # Apenas o pedido 001 atende aos filtros
    assert len(registros) == 1

    pedido = registros[0]

    assert pedido["id_pedido"] == "001"
    assert pedido["uf"] == "SP"
    assert pedido["forma_pagamento"] == "PIX"
    assert pedido["valor_total"] == 200.0
    assert pedido["data_pedido"] == date(2025, 3, 10)

    assert resultado.columns == [
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total",
        "data_pedido"
    ]
