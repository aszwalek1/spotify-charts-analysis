from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType

import seaborn as sns
import matplotlib.pyplot as plt


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

df.createOrReplaceTempView('charts')

# # total of streams of top 200 songs by country 
spark.sql('''
          SELECT region, SUM(streams) AS total_streams 
          FROM charts 
          GROUP BY region 
          ORDER BY total_streams DESC;
          ''').show(10)


# # which artist was in top 200 most often
spark.sql('''
          SELECT artist, COUNT(artist) AS count
          FROM charts 
          GROUP BY artist
          ORDER BY count DESC;
          ''').show(10)

# most streamed songs
spark.sql('''
          SELECT title, SUM(streams) AS total_streams
          FROM charts 
          GROUP BY title
          ORDER BY total_streams DESC;
          ''').show(10)

# most streamed songs in 2021
spark.sql('''
          SELECT title, artist, SUM(streams) AS total_streams
          FROM charts 
          WHERE YEAR(date) = YEAR('2021')
          GROUP BY title, artist
          ORDER BY total_streams DESC;
          ''').show(10)

# rank of 3 most streamed songs by date
most_streamed_songs_rank = spark.sql('''
          SELECT title, rank, date
          FROM charts 
          WHERE YEAR(date) = YEAR('2021') 
          AND title IN ('STAY (with Justin Bieber)', 'drivers license', 'MONTERO (Call Me By Your Name)')
          AND chart = 'top200';
          ''').toPandas()
print(most_streamed_songs_rank.head())

# graph - rank of 10 most streamed songs in 2021

fig, axes = plt.subplots(figsize=(18,7))
axes.set_ylim(200,1)
sns.lineplot(x='date', y='rank', data=most_streamed_songs_rank, hue='title', ci=None).set_title('Rank trends in top 3 most streamed songs in 2021')
plt.show()
