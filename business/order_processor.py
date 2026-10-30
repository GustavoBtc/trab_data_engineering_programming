
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType
)


class OrderProcessor:

    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def get_result_schema(self) -> StructType:
        """Define o schema explicito do relatorio final."""

        return StructType([
            StructField("id_pedido", StringType(), True),
            StructField("uf", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("valor_total", DoubleType(), True),
            StructField("data_pedido", DateType(), True)
        ])

    def process(
        self,
        df_pedidos: DataFrame,
        df_pagamentos: DataFrame
    ) -> DataFrame:

        try:
            self.logger.info("Iniciando processamento")

            # 1. Filtra pedidos realizados em 2025
            pedidos_2025 = df_pedidos.filter(
                (F.to_date("DATA_CRIACAO") >= F.lit("2025-01-01")) &
                (F.to_date("DATA_CRIACAO") < F.lit("2026-01-01"))
            )

            # 2. Filtra pagamentos recusados e legitimos
            pagamentos_validos = df_pagamentos.filter(
                (F.col("status") == False) &
                (F.col("avaliacao_fraude.fraude") == False)
            )

            # 3. Relaciona pedidos com pagamentos
            pedidos = pedidos_2025.alias("p")
            pagamentos = pagamentos_validos.alias("pg")

            df_join = pedidos.join(
                pagamentos,
                F.col("p.ID_PEDIDO") == F.col("pg.id_pedido"),
                "inner"
            )

            # 4. Seleciona e calcula as colunas
            df_resultado = df_join.select(
                F.col("p.ID_PEDIDO").alias("id_pedido"),
                F.col("p.UF").alias("uf"),
                F.col("pg.forma_pagamento"),
                (
                    F.col("p.VALOR_UNITARIO") *
                    F.col("p.QUANTIDADE")
                ).cast(DoubleType()).alias("valor_total"),
                F.to_date(
                    F.col("p.DATA_CRIACAO")
                ).alias("data_pedido")
            )

            # 5. Ordena o relatorio
            df_resultado = df_resultado.orderBy(
                "uf",
                "forma_pagamento",
                "data_pedido"
            )

            # 6. Valida o schema do relatorio
            schema_esperado = self.get_result_schema()

            if df_resultado.schema != schema_esperado:
                raise ValueError(
                    "Schema do relatorio diferente do esperado."
                    f"\nEsperado: {schema_esperado.simpleString()}"
                    f"\nObtido: {df_resultado.schema.simpleString()}"
                )

            self.logger.info("Schema do relatorio validado")
            self.logger.info("Processamento concluido")

            return df_resultado

        except Exception:
            self.logger.exception(
                "Erro durante o processamento dos pedidos"
            )
            raise
