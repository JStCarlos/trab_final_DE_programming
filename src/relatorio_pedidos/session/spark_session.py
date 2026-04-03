from __future__ import annotations

from pyspark.sql import SparkSession


class SparkSessionManager:
    @staticmethod
    def get_spark_session(app_name: str, master: str = "local[*]") -> SparkSession:
        return SparkSession.builder.appName(app_name).master(master).getOrCreate()
