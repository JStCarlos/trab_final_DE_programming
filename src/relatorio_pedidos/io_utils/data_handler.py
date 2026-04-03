"""Leitura e escrita de dados (padrão ``DataHandler`` das aulas)."""

from __future__ import annotations

import logging
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from src.relatorio_pedidos.io_utils.schemas import PAGAMENTOS_SCHEMA, PEDIDOS_SCHEMA

logger = logging.getLogger(__name__)


class DataHandler:
    """Responsável por carregar pedidos, pagamentos e gravar Parquet."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def _get_schema_pedidos(self) -> StructType:
        return PEDIDOS_SCHEMA

    def _get_schema_pagamentos(self) -> StructType:
        return PAGAMENTOS_SCHEMA

    def load_pedidos(
        self,
        path: str,
        compression: str,
        header: bool,
        sep: str,
    ) -> DataFrame:
        """Carrega CSV gzip de pedidos com schema explícito e modo FAILFAST."""
        try:
            schema = self._get_schema_pedidos()
            df = (
                self.spark.read.option("compression", compression)
                .option("mode", "FAILFAST")
                .option("header", header)
                .option("sep", sep)
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
                .schema(schema)
                .csv(path)
            )
            if df.isEmpty():
                logger.warning("Arquivo de pedidos lido sem registros: %s", path)
            else:
                logger.info("Leitura de pedidos concluída (schema explícito, FAILFAST): %s", path)
            return df
        except AnalysisException as exc:
            logger.error("Erro de leitura Spark (pedidos): %s", exc)
            raise
        except Py4JJavaError as exc:
            logger.critical("Erro na JVM ao ler pedidos: %s", exc)
            raise

    def load_pagamentos(self, path: str) -> DataFrame:
        """Carrega JSON gzip de pagamentos com schema explícito."""
        try:
            schema = self._get_schema_pagamentos()
            df = (
                self.spark.read.option("compression", "gzip")
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
                .schema(schema)
                .json(path)
            )
            if df.isEmpty():
                logger.warning("Arquivo(s) de pagamentos lidos sem registros: %s", path)
            else:
                logger.info("Leitura de pagamentos concluída (schema explícito): %s", path)
            return df
        except AnalysisException as exc:
            logger.error("Erro de leitura Spark (pagamentos): %s", exc)
            raise
        except Py4JJavaError as exc:
            logger.critical("Erro na JVM ao ler pagamentos: %s", exc)
            raise

    def write_parquet(
        self,
        dataframe: DataFrame,
        path: str,
        mode: str,
        compression: str = "snappy",
    ) -> None:
        """Persiste o DataFrame em Parquet."""
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Iniciando escrita Parquet: destino=%s, partitions de escrita padrão do Spark",
            path,
        )
        (
            dataframe.write.mode(mode)
            .format("parquet")
            .option("compression", compression)
            .save(str(out))
        )
        logger.info("Escrita Parquet finalizada: %s", path)
