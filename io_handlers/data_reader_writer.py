
"""Leitura e escrita de dados com schemas explícitos."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    IntegerType
)


class DataReaderWriter:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_pedidos_schema(self) -> StructType:
        return StructType([
            StructField("ID_PEDIDO", StringType(), True),
            StructField("PRODUTO", StringType(), True),
            StructField("VALOR_UNITARIO", DoubleType(), True),
            StructField("QUANTIDADE", IntegerType(), True),
            StructField("DATA_CRIACAO", StringType(), True),
            StructField("UF", StringType(), True),
            StructField("ID_CLIENTE", StringType(), True)
        ])

    def get_pagamentos_schema(self) -> StructType:
        fraude_schema = StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score", DoubleType(), True)
        ])

        return StructType([
            StructField("id_pedido", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("valor_pagamento", DoubleType(), True),
            StructField("status", BooleanType(), True),
            StructField("data_processamento", StringType(), True),
            StructField(
                "avaliacao_fraude",
                fraude_schema,
                True
            )
        ])

    def read_pedidos(self, path: str) -> DataFrame:
        return (
            self.spark.read
            .option("header", "true")
            .option("sep", ";")
            .schema(self.get_pedidos_schema())
            .csv(path)
        )

    def read_pagamentos(self, path: str) -> DataFrame:
        return (
            self.spark.read
            .schema(self.get_pagamentos_schema())
            .json(path)
        )

    def write_parquet(
        self,
        df: DataFrame,
        output_path: str
    ) -> None:
        df.write.mode("overwrite").parquet(output_path)
