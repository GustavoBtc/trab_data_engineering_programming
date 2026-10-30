# Data Engineering Programming — Trabalho Final

## 1. Objetivo

Desenvolver uma aplicação PySpark para identificar pedidos realizados em 2025 cujos pagamentos foram recusados (`status = false`), mas classificados como legítimos pela avaliação de fraude (`fraude = false`).

O resultado é gravado em formato Parquet.

## 2. Tecnologias utilizadas

- Python 3.11 ou superior
- Apache Spark / PySpark 4.x
- Pytest
- GitHub Codespaces
- Setuptools

## 3. Estrutura do projeto

```text
trab_data_engineering_programming/
├── business/
│   └── order_processor.py
├── config/
│   └── app_config.py
├── io_handlers/
│   └── data_reader_writer.py
├── pipeline/
│   └── pipeline_orchestrator.py
├── spark/
│   └── spark_session.py
├── tests/
│   └── test_order_processor.py
├── data/
│   ├── pedidos/
│   ├── pagamentos/
│   └── output/
├── main.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

## 4. Fontes de dados

**Pedidos (CSV):**

https://github.com/infobarbosa/datasets-csv-pedidos

**Pagamentos (JSON):**

https://github.com/infobarbosa/dataset-json-pagamentos

Os arquivos de entrada devem ser colocados nas pastas `data/pedidos/` e `data/pagamentos/`, respectivamente.

A aplicação utiliza schemas explícitos para a leitura dos datasets.

## 5. Regras de negócio

O processamento executa as seguintes etapas:

1. Lê os datasets de pedidos e pagamentos.
2. Filtra pedidos criados no ano de 2025.
3. Seleciona pagamentos recusados (`status = false`).
4. Seleciona avaliações legítimas (`avaliacao_fraude.fraude = false`).
5. Relaciona os datasets pelo identificador do pedido.
6. Calcula o valor total multiplicando o valor unitário pela quantidade.
7. Seleciona as cinco colunas do relatório.
8. Ordena por UF, forma de pagamento e data do pedido.
9. Grava o resultado em formato Parquet.

## 6. Instalação

Pré-requisitos: Python 3.11 ou superior, Java compatível com a versão do PySpark utilizada e acesso aos datasets.

Na raiz do projeto, execute:

```bash
python -m pip install -r requirements.txt
python -m pip install -e ".[test]"
```

## 7. Execução

Execute o pipeline completo:

```bash
python main.py
```

O resultado será gravado em:

```text
data/output/relatorio_pedidos_recusados_2025/
```

O Spark gera um diretório contendo arquivos `part-*.parquet` e arquivos auxiliares.

## 8. Testes unitários

Execute:

```bash
python -m pytest
```

O teste verifica os filtros de pagamento, a seleção do ano de 2025, o cálculo do valor total e as colunas do relatório.

## 9. Resultado da execução de referência

Na execução realizada em 30/10/2026, o relatório retornou **1.473 registros**.

O resultado contém:

- `id_pedido`
- `uf`
- `forma_pagamento`
- `valor_total`
- `data_pedido`

## 10. Arquitetura

A aplicação utiliza Programação Orientada a Objetos e Injeção de Dependências.

O `main.py` funciona como *Aggregation Root*, responsável pela criação e injeção das dependências.

O `PipelineOrchestrator` coordena a leitura, o processamento e a escrita dos dados.

O `OrderProcessor` encapsula as regras de negócio e utiliza logging e tratamento de exceções.

## 11. Referência acadêmica

Disciplina: Data Engineering Programming

Professor: Marcelo Barbosa Pinto

Material de apoio:

https://github.com/infobarbosa/pyspark-poo
