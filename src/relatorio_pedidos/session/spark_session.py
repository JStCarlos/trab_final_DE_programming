"""Gerenciamento da SparkSession (padrão das aulas)."""

from __future__ import annotations

from pyspark.sql import SparkSession


class SparkSessionManager:
    """Cria a sessão Spark com ``appName`` e ``master`` configuráveis."""

    @staticmethod
    def get_spark_session(app_name: str, master: str = "local[*]") -> SparkSession:
        """
        Cria e retorna uma SparkSession.

        :param app_name: Nome da aplicação no cluster.
        :param master: URI do master (ex.: ``local[*]``, ``spark://host:7077``).
        """
        return SparkSession.builder.appName(app_name).master(master).getOrCreate()
