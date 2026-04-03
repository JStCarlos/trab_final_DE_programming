from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from relatorio_pedidos.config.settings import Settings
from relatorio_pedidos.io_utils.data_handler import DataHandler
from relatorio_pedidos.processing.transformations import Transformation

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        data_handler: DataHandler,
        transformation: Transformation,
        settings: Settings,
    ) -> None:
        self._data_handler = data_handler
        self._transformation = transformation
        self._settings = settings

    def run(self) -> None:
        logger.info("Pipeline iniciado")
        s = self._settings

        logger.info("Carregando pedidos de: %s", s.pedidos_path)
        pedidos_df = self._data_handler.load_pedidos(
            path=str(s.pedidos_path),
            compression=s.pedidos_compression,
            header=s.pedidos_header,
            sep=s.pedidos_sep,
        )
        logger.info("Pedidos: colunas=%s", pedidos_df.columns)

        logger.info("Carregando pagamentos de: %s", s.pagamentos_path)
        pagamentos_df = self._data_handler.load_pagamentos(str(s.pagamentos_path))
        logger.info("Pagamentos: colunas=%s", pagamentos_df.columns)

        logger.info("Executando regras de negócio (Transformation)")
        relatorio_df = self._transformation.build_relatorio_pedidos(pedidos_df, pagamentos_df)
        logger.info(
            "Relatório pronto: colunas=%s | schema=%s",
            relatorio_df.columns,
            relatorio_df.schema.simpleString(),
        )

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_output = Path(s.output_path) / stamp
        run_output_str = str(run_output.resolve())

        logger.info(
            "Gravando Parquet em %s (mode=%s, compression=%s)",
            run_output_str,
            s.parquet_write_mode,
            s.parquet_compression,
        )
        self._data_handler.write_parquet(
            relatorio_df,
            path=run_output_str,
            mode=s.parquet_write_mode,
            compression=s.parquet_compression,
        )

        logger.info("Lendo relatório gravado para validação e amostra (20 linhas)")
        relatorio_lido = self._data_handler.read_parquet(run_output_str)
        relatorio_lido.show(20, truncate=False)

        logger.info("Pipeline concluído com sucesso")
