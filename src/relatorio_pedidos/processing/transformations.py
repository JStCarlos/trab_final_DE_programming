"""Lógica de negócio: transformações do relatório (classe ``Transformation`` das aulas)."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from relatorio_pedidos.io_utils.schemas import RELATORIO_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


class Transformation:
    """
    Regras de negócio: pedidos de 2025 com pagamento recusado e fraude legítima,
    com ordenação exigida pelo trabalho.
    """

    def build_relatorio_pedidos(self, pedidos: DataFrame, pagamentos: DataFrame) -> DataFrame:
        try:
            logger.info(
                "Iniciando relatório: filtro ano=2025, pagamentos status=false e fraude=false"
            )
            logger.info("Agregando itens de pedido por id_pedido")
            linhas = pedidos.withColumn(
                "valor_linha", F.col("valor_unitario") * F.col("quantidade")
            )
            pedidos_por_id = linhas.groupBy("id_pedido").agg(
                F.sum("valor_linha").alias("valor_total"),
                F.max("uf").alias("uf"),
                F.min("data_criacao").alias("data_pedido"),
            )

            logger.info("Filtrando pedidos do ano de 2025")
            pedidos_2025 = pedidos_por_id.filter(F.year(F.col("data_pedido")) == F.lit(2025))

            logger.info(
                "Filtrando pagamentos com status=false e avaliacao_fraude.fraude=false"
            )
            pagamentos_alvo = pagamentos.filter(
                (F.col("status") == F.lit(False))
                & (F.col("avaliacao_fraude.fraude") == F.lit(False))
            )

            logger.info("Realizando join inner entre pedidos (2025) e pagamentos filtrados")
            joined = pedidos_2025.join(pagamentos_alvo, on="id_pedido", how="inner")

            logger.info("Selecionando colunas, ordenando (UF, forma_pagamento, data_pedido)")
            relatorio = (
                joined.select(
                    F.col("id_pedido").cast("string").alias("id_pedido"),
                    F.col("uf").cast("string").alias("uf"),
                    F.col("forma_pagamento").cast("string").alias("forma_pagamento"),
                    F.col("valor_total").cast("double").alias("valor_total"),
                    F.col("data_pedido").cast("timestamp").alias("data_pedido"),
                )
                .orderBy(
                    F.col("uf").asc_nulls_last(),
                    F.col("forma_pagamento").asc_nulls_last(),
                    F.col("data_pedido").asc_nulls_last(),
                )
            )

            logger.info(
                "Aplicando schema explícito de saída (RELATORIO_SCHEMA): %s",
                RELATORIO_SCHEMA.simpleString(),
            )
            cast_cols = [
                F.col(field.name).cast(field.dataType).alias(field.name)
                for field in RELATORIO_SCHEMA.fields
            ]
            relatorio_final = relatorio.select(*cast_cols)
            logger.info("Relatório materializado com schema de saída definido")
            return relatorio_final
        except Exception:
            logger.exception("Falha ao construir o relatório de pedidos")
            raise
