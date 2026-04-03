"""Testes de integração Spark da ``Transformation`` (Linux / AWS Academy)."""

from __future__ import annotations

import sys
from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession

from relatorio_pedidos.io_utils.schemas import (
    PAGAMENTOS_SCHEMA,
    PEDIDOS_SCHEMA,
    RELATORIO_SCHEMA,
)
from relatorio_pedidos.processing.transformations import Transformation

# Em Linux (ex.: AWS Academy) a suíte roda por completo. No Windows, agregações
# locais com PySpark costumam falhar no worker; use WSL ou ignore os skips.
pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Integração Spark local instável no Windows; execute pytest em Linux (AWS Academy).",
)


def _build_spark_session() -> SparkSession:
    builder = SparkSession.builder.master("local[1]").appName("pytest-relatorio-pedidos")
    if sys.platform == "win32":
        builder = builder.config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        )
    return builder.getOrCreate()


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = _build_spark_session()
    yield session
    session.stop()


def test_build_relatorio_pedidos_inclui_apenas_2025_com_pagamento_recusado_e_legitimo(
    spark: SparkSession,
) -> None:
    pedidos = spark.createDataFrame(
        [
            (
                "p-ok",
                "MONITOR",
                100.0,
                2,
                datetime(2025, 3, 1, 12, 0, 0),
                "SP",
                10,
            ),
            (
                "p-fora-ano",
                "MOUSE",
                50.0,
                1,
                datetime(2024, 3, 1, 12, 0, 0),
                "RJ",
                11,
            ),
        ],
        schema=PEDIDOS_SCHEMA,
    )
    pagamentos = spark.createDataFrame(
        [
            (
                "p-ok",
                "PIX",
                200.0,
                False,
                None,
                Row(fraude=False, score=0.2),
            ),
            (
                "p-fora-ano",
                "BOLETO",
                50.0,
                False,
                None,
                Row(fraude=False, score=0.1),
            ),
        ],
        schema=PAGAMENTOS_SCHEMA,
    )

    transformer = Transformation()
    relatorio = transformer.build_relatorio_pedidos(pedidos, pagamentos)

    assert relatorio.schema == RELATORIO_SCHEMA
    assert relatorio.count() == 1
    linha = relatorio.first()
    assert linha.id_pedido == "p-ok"
    assert linha.uf == "SP"
    assert linha.forma_pagamento == "PIX"
    assert linha.valor_total == 200.0
    assert linha.data_pedido == datetime(2025, 3, 1, 12, 0, 0)


def test_build_relatorio_pedidos_ordenacao_uf_forma_pagamento_data(spark: SparkSession) -> None:
    pedidos = spark.createDataFrame(
        [
            (
                "a",
                "P",
                10.0,
                1,
                datetime(2025, 1, 2, 0, 0, 0),
                "RJ",
                1,
            ),
            (
                "b",
                "P",
                10.0,
                1,
                datetime(2025, 1, 1, 0, 0, 0),
                "RJ",
                2,
            ),
            (
                "c",
                "P",
                10.0,
                1,
                datetime(2025, 1, 1, 0, 0, 0),
                "SP",
                3,
            ),
        ],
        schema=PEDIDOS_SCHEMA,
    )
    pagamentos = spark.createDataFrame(
        [
            ("a", "PIX", 10.0, False, None, Row(fraude=False, score=0.1)),
            ("b", "BOLETO", 10.0, False, None, Row(fraude=False, score=0.1)),
            ("c", "PIX", 10.0, False, None, Row(fraude=False, score=0.1)),
        ],
        schema=PAGAMENTOS_SCHEMA,
    )

    transformer = Transformation()
    relatorio = transformer.build_relatorio_pedidos(pedidos, pagamentos)
    assert relatorio.schema == RELATORIO_SCHEMA

    ordenado = relatorio.collect()
    chaves = [(r.uf, r.forma_pagamento, r.data_pedido) for r in ordenado]
    assert chaves == sorted(chaves)
