from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from relatorio_pedidos.processing.transformations import Transformation


@pytest.fixture
def spark_context_only() -> SparkSession:
    if sys.platform == "win32":
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    builder = SparkSession.builder.master("local[1]").appName("pytest-transformation-unit")
    if sys.platform == "win32":
        builder = builder.config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        )
    spark = builder.getOrCreate()
    yield spark
    spark.stop()


def test_transformation_registra_excecao_ao_falhar_na_transformacao(
    spark_context_only: SparkSession,
) -> None:
    _ = spark_context_only

    pedidos = MagicMock()
    pedidos.withColumn.side_effect = RuntimeError("falha simulada")

    with patch("relatorio_pedidos.processing.transformations.logger") as log_mock:
        transformer = Transformation()
        with pytest.raises(RuntimeError, match="falha simulada"):
            transformer.build_relatorio_pedidos(pedidos, MagicMock())

    log_mock.exception.assert_called_once()
