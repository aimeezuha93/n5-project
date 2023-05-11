from pyspark.sql import SparkSession
from src.python.services.utils import DataHandler


class SparkDataHandler(DataHandler):
    def __init__(self):
        super().__init__()
        self.spark = (
            SparkSession.builder.appName("n5-project")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "5g")
            .getOrCreate()
        )

    def _extract_data(self, path):
        df = self.spark.read.csv(path, header=True)
        return df
