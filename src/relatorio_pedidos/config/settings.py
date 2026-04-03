"""Configurações centralizadas (YAML + overrides por variáveis de ambiente)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


def _project_root() -> Path:
    """
    Raiz do repositório (pai de ``src``).

    Pode ser fixada por ``RELATORIO_PROJECT_ROOT`` ou ``PROJECT_ROOT`` quando o
    processo não roda com CWD na raiz (IDE, spark-submit a partir de outra pasta).
    """
    for key in ("RELATORIO_PROJECT_ROOT", "PROJECT_ROOT"):
        raw = os.environ.get(key)
        if raw:
            return Path(raw).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


def carregar_config(path: str | Path | None = None) -> dict[str, Any]:
    """Carrega YAML. Caminhos relativos são resolvidos a partir da raiz do projeto."""
    root = _project_root()
    if path is None:
        env_yaml = os.environ.get("RELATORIO_SETTINGS_YAML")
        if env_yaml:
            cfg_path = Path(env_yaml).expanduser()
        else:
            cfg_path = root / "config" / "settings.yaml"
    else:
        cfg_path = Path(path).expanduser()
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()
    with open(cfg_path, encoding="utf-8") as file:
        data: dict[str, Any] = yaml.safe_load(file)
        return data


def _resolve_path(value: str, root: Path) -> Path:
    p = Path(value).expanduser()
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
