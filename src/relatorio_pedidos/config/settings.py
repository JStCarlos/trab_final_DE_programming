"""Configurações centralizadas (YAML + overrides por variáveis de ambiente)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def _project_root() -> Path:
    """Raiz do repositório (pai de ``src``)."""
    return Path(__file__).resolve().parents[3]


def carregar_config(path: str | Path | None = None) -> dict[str, Any]:
    """Carrega o arquivo YAML de configuração (padrão: ``config/settings.yaml`` na raiz)."""
    root = _project_root()
    cfg_path = Path(path) if path is not None else root / "config" / "settings.yaml"
    with open(cfg_path, encoding="utf-8") as file:
        data: dict[str, Any] = yaml.safe_load(file)
        return data


def _resolve_path(value: str, root: Path) -> Path:
    p = Path(value)
    return p if p.is_absolute() else (root / p).resolve()


@dataclass(frozen=True)
class Settings:
    """Classe de configuração utilizada no fluxo principal (``main.py``)."""

    spark_app_name: str
    spark_master: str
    pedidos_path: Path
    pagamentos_path: Path
    output_path: Path
    pedidos_compression: str
    pedidos_header: bool
    pedidos_sep: str
    parquet_write_mode: str
    parquet_compression: str

    @classmethod
    def load(cls, yaml_path: str | Path | None = None) -> Settings:
        root = _project_root()
        cfg = carregar_config(yaml_path)

        spark = cfg.get("spark", {})
        paths = cfg.get("paths", {})
        file_opts = cfg.get("file_options", {}).get("pedidos_csv", {})
        parquet = cfg.get("parquet", {})

        header = file_opts.get("header", True)
        if isinstance(header, str):
            header = header.lower() in ("true", "1", "yes")

        return cls(
            spark_app_name=os.environ.get("SPARK_APP_NAME", spark.get("app_name", "relatorio-pedidos")),
            spark_master=os.environ.get("SPARK_MASTER", spark.get("master", "local[*]")),
            pedidos_path=_resolve_path(
                os.environ.get("PEDIDOS_INPUT_PATH", paths.get("pedidos", "data/pedidos")),
                root,
            ),
            pagamentos_path=_resolve_path(
                os.environ.get("PAGAMENTOS_INPUT_PATH", paths.get("pagamentos", "data/pagamentos")),
                root,
            ),
            output_path=_resolve_path(
                os.environ.get("RELATORIO_OUTPUT_PATH", paths.get("output", "output/relatorio_pedidos")),
                root,
            ),
            pedidos_compression=str(file_opts.get("compression", "gzip")),
            pedidos_header=bool(header),
            pedidos_sep=str(file_opts.get("sep", ";")),
            parquet_write_mode=os.environ.get("PARQUET_WRITE_MODE", parquet.get("write_mode", "overwrite")),
            parquet_compression=str(parquet.get("compression", "snappy")),
        )
