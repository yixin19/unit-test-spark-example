from pyspark.sql import DataFrame, SparkSession


# We have a csv file located in raw directory
# We want to clean pitags name in this file and load the data into bronze layer into parquet format.
# Perform the following step using spark to do this operation

# 1. Extract: from csv ./raw/pitags.csv
# 2. Transform: if there's "." at the end of pitag name, remove last ".".
#   hint: use expresion `when` and `substring`
# 3. load the data into bronze layer

class PitagsJob:
    def __init__(self):
        self.spark = SparkSession.builder.master("local[1]") \
            .appName("Pitags raw to bronze") \
            .getOrCreate()

    def extract(self) -> DataFrame:
        return self.spark.read.options(inferSchema='True', delimiter=';', header='True').format("csv").load(
            "../raw/pitags.csv")

    def transform(self, pitags_df: DataFrame) -> DataFrame:
        pass

    def load(self, pitags_df: DataFrame):
        pass

    def launch(self):
        pitags_df = self.extract()
        pitags_df = self.transform(pitags_df=pitags_df)
        self.load(pitags_df=pitags_df)


if __name__ == '__main__':
    PitagsJob().launch()
