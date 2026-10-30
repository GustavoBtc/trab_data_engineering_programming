
import os

from pyspark.sql import SparkSession


class SparkSessionManager:

    @staticmethod
    def get_session(
        app_name="DataEngineeringProgramming"
    ) -> SparkSession:

        builder = SparkSession.builder.appName(app_name)

        # Permite configurar o Spark pelo ambiente
        master = os.getenv("SPARK_MASTER")

        if master:
            builder = builder.master(master)

        spark = builder.getOrCreate()

        return spark
