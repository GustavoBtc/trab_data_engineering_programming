
from config.app_config import AppConfig
from spark.spark_session import SparkSessionManager
from io_handlers.data_reader_writer import DataReaderWriter
from business.order_processor import OrderProcessor
from pipeline.pipeline_orchestrator import PipelineOrchestrator


def main():

    # 1. Instancia a configuracao
    config = AppConfig()

    # 2. Cria a sessao Spark
    spark = SparkSessionManager.get_session(
        "DataEngineeringProgramming"
    )

    try:
        # 3. Instancia as dependencias
        data_io = DataReaderWriter(spark)
        processor = OrderProcessor()

        # 4. Injeta as dependencias no orquestrador
        pipeline = PipelineOrchestrator(
            data_io,
            processor
        )

        # 5. Executa o pipeline
        pipeline.run(
            pedidos_path=config.PEDIDOS_PATH,
            pagamentos_path=config.PAGAMENTOS_PATH,
            output_path=config.OUTPUT_PATH
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
