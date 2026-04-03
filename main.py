from __future__ import annotations

import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
_SRC = _REPO_ROOT / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from relatorio_pedidos.config.settings import Settings
from relatorio_pedidos.io_utils.data_handler import DataHandler
from relatorio_pedidos.pipeline.pipeline import Pipeline
from relatorio_pedidos.processing.transformations import Transformation
from relatorio_pedidos.session.spark_session import SparkSessionManager


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler()],
        force=True,
    )
    logging.info("Logging configurado.")


def main() -> None:
    logger = logging.getLogger(__name__)
    settings = Settings.load()
    logger.info(
        "Configuração carregada: master=%s | pedidos=%s | pagamentos=%s | saída=%s",
        settings.spark_master,
        settings.pedidos_path,
        settings.pagamentos_path,
        settings.output_path,
    )
    spark = None
    try:
        logger.info("Iniciando SparkSession: app_name=%s", settings.spark_app_name)
        spark = SparkSessionManager.get_spark_session(
            app_name=settings.spark_app_name,
            master=settings.spark_master,
        )
        data_handler = DataHandler(spark)
        transformation = Transformation()
        pipeline = Pipeline(
            data_handler=data_handler,
            transformation=transformation,
            settings=settings,
        )
        pipeline.run()
    except Exception as exc:
        logger.error("Falha crítica no pipeline: %s", exc, exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    setup_logging()
    main()
