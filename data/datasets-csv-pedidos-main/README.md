# Dataset de pedidos (CSV gzip)

## Leiaute dos arquivos

- **Separador**: `;`
- **Header**: sim
- **Compressão**: gzip

### `pedidos*.csv.gz`

| Atributo       | Tipo   | Descrição                                      |
| -------------- | ------ | ---------------------------------------------- |
| ID_PEDIDO      | UUID   | Identificador do pedido                        |
| PRODUTO        | string | Nome do produto no pedido                      |
| VALOR_UNITARIO | float  | Valor unitário do produto                      |
| QUANTIDADE     | long   | Quantidade do produto no pedido                |
| DATA_CRIACAO   | date   | Data/hora de criação do pedido                 |
| UF             | string | Sigla da unidade federativa (estado)           |
| ID_CLIENTE     | long   | Identificador do cliente                       |

### Exemplo de linhas

```
id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente
fdd7933e-ce3a-4475-b29d-f239f491a0e7;MONITOR;600;3;2024-01-01T22:26:32;RO;12414
```

Os arquivos reais devem ficar em `data/pedidos/` **dentro deste repositório de dataset**, ou no projeto consumidor em `data/datasets-csv-pedidos-main/data/pedidos/`, conforme o `config/settings.yaml` do pipeline.

---

## Pipeline de relatório (projeto na pasta pai)

Este CSV é consumido pelo projeto **trabalho final** (PySpark) que fica na raiz do repositório principal, **um nível acima** de `data/` (ex.: `trab_final_DE_programming/`). Abaixo: instalação em máquina virtual Linux (ex.: Ubuntu na AWS) e execução.

### 1. Requisitos na máquina virtual

| Componente | Observação |
| ---------- | ---------- |
| **Ubuntu** (ou outra distro Linux recente) | Ambiente comum em cloud / Academy |
| **Java 11 ou 17** | Necessário para a JVM do Spark (`sudo apt install openjdk-17-jdk` ou equivalente) |
| **Python 3.10+** | `python3 --version` |
| **Git** | Para clonar o repositório do trabalho |

Confira Java:

```bash
java -version
```

Confira Python:

```bash
python3 --version
```

### 2. Clonar o repositório do trabalho e entrar na pasta

```bash
cd ~/environment
git clone <URL_DO_SEU_REPOSITORIO> trab_final_DE_programming
cd trab_final_DE_programming
```

Substitua `<URL_DO_SEU_REPOSITORIO>` pela URL pública do GitHub do grupo.

### 3. Dados de pedidos e pagamentos

- **Pedidos**: arquivos `*.csv.gz` neste layout, no caminho esperado pelo projeto, por exemplo:  
  `data/datasets-csv-pedidos-main/data/pedidos/`
- **Pagamentos** (JSON gzip): em `data/dataset-json-pagamentos-main/data/pagamentos/`

Se as pastas forem diferentes, ajuste `paths.pedidos` e `paths.pagamentos` em `config/settings.yaml` na raiz do projeto (caminhos relativos à raiz do repositório).

### 4. Ambiente virtual Python e dependências

```bash
cd ~/environment/trab_final_DE_programming
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"
```

- `-e ".[dev]"` instala o pacote em modo editável e o **pytest** para testes.
- Se preferir só executar o pipeline (sem dependência explícita do extras): `pip install -e .`

### 5. Executar o pipeline

Na **raiz** do repositório (`trab_final_DE_programming`), com o venv ativado:

```bash
source .venv/bin/activate
cd ~/environment/trab_final_DE_programming
python main.py
```

O `main.py` adiciona `src/` ao `sys.path`, então o comando acima funciona mesmo sem reinstalar o pacote a cada alteração.

Se o terminal não estiver com *working directory* na raiz do projeto, defina a raiz explicitamente:

```bash
export RELATORIO_PROJECT_ROOT="$HOME/environment/trab_final_DE_programming"
python "$RELATORIO_PROJECT_ROOT/main.py"
```

### 6. Onde fica o relatório Parquet

- Pasta base configurada em `paths.output` (padrão: `output/relatorio_pedidos`).
- **Cada execução** grava em uma subpasta com data e hora:  
  `output/relatorio_pedidos/AAAAMMDD_HHMMSS/`  
  (ex.: `output/relatorio_pedidos/20250403_143022/`).

Ao final, o pipeline **lê de volta** esse Parquet e imprime as **20 primeiras linhas** no log (saída do Spark).

### 7. Ler o resultado manualmente no PySpark

Abra o shell Python do Spark (não cole Python direto no bash):

```bash
cd ~/environment/trab_final_DE_programming
pyspark
```

No prompt Python:

```python
path = "output/relatorio_pedidos/20250403_143022"
df = spark.read.parquet(path)
df.printSchema()
df.show(20, truncate=False)
```

Troque `path` pelo nome real da pasta criada na última execução.

### 8. Testes automatizados

```bash
cd ~/environment/trab_final_DE_programming
source .venv/bin/activate
pytest -v
```

Em Linux costumam rodar todos os testes. Em Windows, parte da integração Spark pode ser ignorada (`skipped`).

### 9. Variáveis de ambiente úteis (opcional)

| Variável | Uso |
| -------- | --- |
| `RELATORIO_PROJECT_ROOT` | Raiz do repositório quando o CWD não é a raiz |
| `RELATORIO_SETTINGS_YAML` | Caminho alternativo para o YAML de configuração |
| `PEDIDOS_INPUT_PATH` | Sobrescreve pasta de pedidos |
| `PAGAMENTOS_INPUT_PATH` | Sobrescreve pasta de pagamentos |
| `RELATORIO_OUTPUT_PATH` | Sobrescreve pasta **base** de saída (o timestamp é sempre uma subpasta) |
| `SPARK_MASTER` | Ex.: `local[*]` ou URL do cluster |

Documentação adicional do projeto: `README.md` na raiz do repositório `trab_final_DE_programming`.
