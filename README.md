# iFood Data Architect Case

Pipeline PySpark para ingestao, tratamento e publicacao de dados de Yellow Taxi
da NYC TLC entre janeiro e maio de 2023.

## Visao Geral

Este projeto entrega um fluxo pequeno, executavel e apresentavel para o case:

- leitura dos cinco Parquets mensais de Yellow Taxi;
- validacao e publicacao de uma camada Silver em Delta Lake;
- materializacao de duas agregacoes Gold pedidas no desafio;
- execucao local com Spark + Delta;
- execucao no Databricks com notebooks finos;
- testes unitarios e de integracao para as regras principais.

**Aviso**:
o projeto pode ser executado tanto localmente quanto no Databricks. A opcao
mais recomendada e o Databricks, porque nela voce reaproveita os componentes e
classes implementados neste repositorio e ainda conta com uma visao mais
completa de analise, com queries, views e notebooks de apoio ja disponiveis na
pasta `analysis/`.

Se optar pela execucao no Databricks, lembre-se de copiar os arquivos
`.parquet` para dentro do volume configurado antes de rodar o pipeline.

## Arquitetura

O projeto atual foi mantido propositalmente enxuto para o tamanho do case, mas
ja foi organizado de forma a sugerir uma evolucao natural para uma plataforma
de dados mais completa.

![Evolucao simples da arquitetura da plataforma](assets/imgs/Evolucao%20Simples%20Arquitetura%20Plataforma.png)

O desenho acima deve ser lido como uma orientacao sistemica de evolucao da
arquitetura, e nao como algo 100% entregue neste repositorio neste momento.
Ele representa uma melhoria de projeto: um caminho simples e pragmatica para
expandir a solucao atual para uma plataforma de dados mais robusta.

Pensando nessa evolucao, a plataforma passaria a oferecer:

- conectores para multiplas origens, como arquivos, APIs, bancos e eventos;
- uma zona de ingestao para carga batch e, futuramente, streaming;
- camadas organizadas em arquitetura Medallion, com Landing, Silver e Gold;
- transformacoes e validacoes reutilizaveis ao longo do pipeline;
- schematizacao e padronizacao de contratos de dados;
- tecnologias modernas para processamento e armazenamento, como Spark e Delta Lake.
- Repositórios auxiliares para tratar a plataforma orientado a .yaml, auxiliando na ingestão e transformações de dados de forma facíl para os usuarios da plataforma.

Neste projeto, a entrega implementada hoje cobre uma versao reduzida dessa
visao, concentrada no fluxo local com conector de arquivos, transformacoes
Silver e Gold, persistencia Delta e orquestracao simples.

- `core.connector`: leitura da origem
- `core.transform`: regras de transformacao Silver e Gold
- `core.sync`: persistencia Delta por path ou tabela
- `core.spark`: criacao da sessao Spark local
- `core.runner`: orquestracao local do pipeline


## Estrutura

```text
├── 📁 .agents/                           # recursos auxiliares para agentes no workspace
├── 📁 .codex/                            # configuracoes locais de execucao do Codex
├── 📁 .github/
│   └── 📁 workflows/
│       └── ci.yml                        # pipeline de CI
├── 📁 analysis/
│   ├── __init__.py
│   └── 📁 notebooks/                     # notebooks Python para execucao no Databricks
│       ├── __init__.py
│       ├── 00_setup.py                   # bootstrap de catalogo/schema/volume
│       ├── 01_build_silver.py            # publicacao da camada Silver
│       ├── 02_build_gold.py              # publicacao das agregacoes Gold
│       └── 03_dashboard_walkthrough.py   # apoio para validacao e apresentacao
├── 📁 data/
│   ├── 📁 landing/                       # entrada local esperada pelo pipeline
│   ├── 📁 silver/                        # saida Delta local da camada Silver
│   └── 📁 gold/                          # saidas Delta locais da camada Gold
├── 📁 src/
│   ├── __init__.py
│   └── 📁 core/
│       ├── __init__.py
│       ├── 📁 connector/
│       │   ├── __init__.py
│       │   └── loader.py                 # leitura e uniao dos arquivos mensais
│       ├── 📁 spark/
│       │   ├── __init__.py
│       │   └── spark.py                  # criacao da SparkSession local com Delta
│       ├── 📁 sync/
│       │   ├── __init__.py
│       │   └── sync.py                   # persistencia Delta por path e tabela
│       ├── 📁 transform/
│       │   ├── __init__.py
│       │   └── transform_service.py      # regras Silver e Gold
│       └── runner.py                     # orquestracao local do pipeline
├── 📁 tests/
│   ├── conftest.py                       # fixtures compartilhadas da suite
│   ├── 📁 integration/
│   │   └── test_local_pipeline.py        # pipeline ponta a ponta local
│   └── 📁 unit/
│       ├── test_connector.py             # connector de landing
│       ├── test_gold.py                  # agregacoes Gold
│       ├── test_runner.py                # runner local
│       ├── test_silver.py                # validacoes e contrato Silver
│       ├── test_spark_service.py         # servicos de Spark
│       └── test_sync.py                  # persistencia Delta
├── .gitignore
├── Makefile                              # comandos padronizados de desenvolvimento
├── pyproject.toml                        # empacotamento e configuracao de ferramentas
├── requirements.txt                      # dependencias instaladas no setup local
└── README.md
```

## Requisitos

- Python 3.10
- Java 17
- Make
- ambiente Linux/WSL (Ubuntu)

## Dados de Entrada

O pipeline espera os arquivos abaixo em `data/landing/`:

```text
yellow_tripdata_2023-01.parquet
yellow_tripdata_2023-02.parquet
yellow_tripdata_2023-03.parquet
yellow_tripdata_2023-04.parquet
yellow_tripdata_2023-05.parquet
```

## Setup Local

```bash
make setup
source .venv/bin/activate
```

Observacao:
na primeira execucao do Spark com Delta, o ambiente pode precisar baixar jars
do Maven. Por isso, `make test` e `make run` assumem acesso de rede nessa etapa.

## Comandos Principais

Listar os comandos disponiveis:

```bash
make help
```

Rodar testes:

```bash
make test
```

Rodar validacoes estaticas:

```bash
make lint
```

Aplicar formatacao automatica:

```bash
make format
```

Executar pipeline local:

```bash
make run
```

Abrir shell Spark local:

```bash
make shell
```

No shell, você pode realizar suas verificações! Como

```bash
gold = spark.read.load(path="data/gold/may_hourly_average_passengers")

gold.show()

gold.count()
```

## Saidas Locais

O pipeline publica:

```text
data/silver/yellow_trips
data/gold/monthly_average_total_amount
data/gold/may_hourly_average_passengers
```

## Databricks

Os notebooks em `analysis/notebooks/` representam a execucao no workspace:

- `00_setup.py`: cria catalogo/schema/volume e valida arquivos de entrada
- `01_build_silver.py`: le landing e publica `ifood_case.silver.yellow_trips`
- `02_build_gold.py`: le Silver e publica as duas tabelas Gold
- `03_dashboard_walkthrough.py`: apoio para validacao e dashboard

Objetos usados no Databricks:

- catalogo: `ifood_case`
- schemas: `landing`, `silver`, `gold`
- volume: `/Volumes/ifood_case/landing/source_files`

Os dashboards abaixo foram montados no Databricks a partir das tabelas Gold e
servem como apoio visual para a analise final do case. A ideia foi manter uma
leitura direta dos principais indicadores de volume, sazonalidade e comportamento
medio das corridas.

<p align="center">
  <img src="assets/imgs/Volume%20Total%20Viagens.png" alt="Dashboard com o volume total de viagens" width="48%" />
  <img src="assets/imgs/Corridas%20Mensal.png" alt="Dashboard com corridas por mes" width="48%" />
</p>

<p align="center">
  <img src="assets/imgs/Media%20mensal%20por%20corrida.png" alt="Dashboard com media mensal por corrida" width="48%" />
  <img src="assets/imgs/Media%20de%20pessageiros%20por%20hora%20em%20maio.png" alt="Dashboard com media de passageiros por hora em maio" width="48%" />
</p>

## Testes

A suite cobre:

- transformacoes Silver
- transformacoes Gold
- connector da landing
- servicos de sync
- servicos de Spark
- runner local
- pipeline local completo

Organizacao:

- `tests/unit/`: regras e servicos isolados
- `tests/integration/`: fluxo local ponta a ponta
- `tests/conftest.py`: fixtures compartilhadas, incluindo sessao Spark

## Decisoes Tecnicas

- Escopo restrito a Yellow Taxi, porque o contrato do case usa colunas dessa base
- Delta Lake para Silver e Gold
- full refresh em vez de append
- sem particionamento fisico, porque o volume do case e pequeno
- notebooks finos; regra de negocio concentrada em `src/core`
- OOP leve apenas para sinalizar separacao de responsabilidades

## Observacoes

- O projeto nao tenta simular uma plataforma enterprise completa.
- A modelagem favorece clareza, execucao e defesa tecnica na apresentacao.
- As abstracoes foram mantidas no minimo necessario para sugerir evolucao futura
  para novas fontes de dados e novos ambientes.
