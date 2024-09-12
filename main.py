from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType


custom_schema = StructType([
    StructField("title", StringType(), True),
    StructField("rank", LongType(), True),
    StructField("date", DateType(), True),
    StructField("artist", StringType(), True),
    StructField("url", StringType(), True),
    StructField("region", StringType(), True),
    StructField("chart", StringType(), True),
    StructField("trend", StringType(), True),
    StructField("streams", IntegerType(), True)
])
spark = (SparkSession.builder.config("spark.driver.memory","4g").config("spark.driver.maxResultSize", "4g").getOrCreate())

df = spark.read.csv(path='charts.csv', schema=custom_schema, header=True)

df.printSchema()
df.show(5)