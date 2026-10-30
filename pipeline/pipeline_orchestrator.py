
from io_handlers.data_reader_writer import DataReaderWriter
from business.order_processor import OrderProcessor


class PipelineOrchestrator:

    def __init__(
        self,
        data_io: DataReaderWriter,
        processor: OrderProcessor
    ):
        self.data_io = data_io
        self.processor = processor

    def run(
        self,
        pedidos_path: str,
        pagamentos_path: str,
        output_path: str
    ) -> None:

        # 1. Leitura dos datasets
        print("Iniciando leitura dos dados...")

        df_pedidos = self.data_io.read_pedidos(
            pedidos_path
        )

        df_pagamentos = self.data_io.read_pagamentos(
            pagamentos_path
        )

        # 2. Processamento das regras de negocio
        print("Iniciando processamento dos pedidos...")

        df_resultado = self.processor.process(
            df_pedidos,
            df_pagamentos
        )

        # 3. Exibe o resultado
        print("\n--- RELATORIO FINAL 2025 ---")

        df_resultado.printSchema()

        print(
            f"Total de registros: {df_resultado.count()}"
        )

        df_resultado.show(10, truncate=False)

        # 4. Grava o resultado em Parquet
        print("Gravando relatorio em Parquet...")

        self.data_io.write_parquet(
            df_resultado,
            output_path
        )

        print("Pipeline executado com sucesso!")
