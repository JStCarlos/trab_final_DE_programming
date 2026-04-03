# Relatório de pedidos (PySpark) — Trabalho Final DE Programming

Pipeline PySpark que gera relatório de **pedidos de 2025** com **pagamento recusado** (`status = false`) e **avaliação de fraude legítima** (`avaliacao_fraude.fraude = false`), com join nos dados de pedidos para obter **UF**, **valor total** (soma de `valor_unitario * quantidade`) e **data do pedido**. Saída em **Parquet**, ordenada por **UF**, **forma de pagamento** e **data do pedido**.

A organização de pacotes segue o modelo do repositório de aulas [pyspark-poo](https://github.com/infobarbosa/pyspark-poo): `config` + `settings.yaml`, `session`, `io_utils` (`DataHandler`), `processing` (`Transformation`) e `pipeline` (`Pipeline`).

## Pré-requisitos

- Python **3.10+**
- Java 8/11/17 compatível com a versão do Spark em uso (necessário para o `pyspark` local ou cluster)

## Configuração

Edite **`config/settings.yaml`** na raiz do repositório para apontar caminhos, opções de CSV gzip e modo de escrita Parquet. Valores podem ser sobrescritos por variáveis de ambiente (veja abaixo).

## Estrutura esperada dos dados

Coloque os datasets conforme os repositórios oficiais do professor:

- **Pedidos** (CSV gzip, `;`, com header):  
  `data/datasets-csv-pedidos-main/data/pedidos/`  
  (equivalente a `datasets-csv-pedidos/data/pedidos/` no repositório original)

- **Pagamentos** (JSON gzip, `pagamentos-YYYY-MM-DD.json.gz`):  
  `data/dataset-json-pagamentos-main/data/pagamentos/`

Os leiautes estão nos `README.md` dentro de cada pasta em `data/`.

## Instalação

Na raiz do repositório:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

(Linux/macOS: `source .venv/bin/activate`.)

## Execução local

Com o ambiente virtual ativo, na **raiz** do projeto (para que `config/settings.yaml` e os caminhos relativos funcionem):

```bash
python main.py
```

O relatório será escrito no caminho definido em `paths.output` (padrão `output/relatorio_pedidos/`), em modo **overwrite** por padrão.

### Variáveis de ambiente (opcional)

Sobrescrevem entradas do YAML:

| Variável | Descrição |
|----------|-----------|
| `RELATORIO_PROJECT_ROOT` | Raiz do repositório (pasta que contém `config/` e `src/`). Use se o processo não estiver com *working directory* na raiz (alguns IDEs ou `spark-submit` de outra pasta). Equivale a `PROJECT_ROOT`. |
| `RELATORIO_SETTINGS_YAML` | Caminho absoluto ou relativo à raiz para outro arquivo de configuração YAML. |
| `SPARK_MASTER` | Master do Spark (`local[*]`, `spark://...`, `yarn`, etc.) |
| `SPARK_APP_NAME` | Nome da aplicação no cluster |
| `PEDIDOS_INPUT_PATH` | Diretório ou glob de arquivos de pedidos (absoluto ou relativo à raiz do projeto) |
| `PAGAMENTOS_INPUT_PATH` | Diretório de JSON de pagamentos |
| `RELATORIO_OUTPUT_PATH` | Pasta de saída Parquet |
| `PARQUET_WRITE_MODE` | Modo de escrita (`overwrite`, `append`, …) |

**Desenvolvimento:** instale o pacote em modo editável (`pip install -e .`) e execute `python main.py` a partir da raiz do repo, **ou** defina `RELATORIO_PROJECT_ROOT` apontando para essa raiz. Os imports usam o pacote `relatorio_pedidos` (não `src.relatorio_pedidos`).

## Cluster em containers (Docker / Spark standalone)

1. Use uma imagem oficial **Apache Spark** (ou a do laboratório) com a mesma versão major/minor do PySpark instalado no cliente de submissão, quando possível.
2. Monte o repositório (incluindo `config/settings.yaml`) e a pasta `data/` no driver.
3. Ajuste `spark.master` no YAML ou defina `SPARK_MASTER`.
4. Exemplo:

```bash
spark-submit --master spark://spark-master:7077 main.py
```

Ajuste `--py-files`, `--packages` e rede conforme o compose usado no laboratório.

## Testes

```bash
pytest
```

- Integração: Spark `local[1]` valida `Transformation`, ordenação e **`RELATORIO_SCHEMA`** na saída — rode em **Linux** (ex.: AWS Academy). No **Windows**, esses testes são ignorados (`skipped`) por instabilidade do worker em agregações locais.
- Unitário: mock de falha + verificação de `logger.exception` na `Transformation` (roda em qualquer SO).
- O `conftest.py` define `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` para o mesmo `python` do venv.

## Arquitetura (requisitos do trabalho + modelo das aulas)

- **Schemas explícitos** na leitura (sem `inferSchema`) e **`RELATORIO_SCHEMA`** na saída (colunas finais com `cast` a partir dos tipos do `StructType`); `valor_unitario` como `DoubleType` nos pedidos.
- **`main.py`** como aggregation root: instancia `Settings`, `SparkSessionManager`, `DataHandler`, `Transformation` e `Pipeline` (injeção por construtor).
- **`config`**: classe `Settings` + `carregar_config()` / arquivo `config/settings.yaml`.
- **`session`**: `SparkSessionManager.get_spark_session(app_name, master)`.
- **`io_utils`**: `DataHandler` (leitura com `FAILFAST` em pedidos, escrita Parquet, logging de erros).
- **`processing`**: `Transformation` com `logging`, `basicConfig` e `try`/`except` na construção do relatório.
- **`pipeline`**: `Pipeline` orquestra carga → transformação → gravação.

## Colunas do relatório

1. `id_pedido`
2. `uf`
3. `forma_pagamento`
4. `valor_total`
5. `data_pedido`
