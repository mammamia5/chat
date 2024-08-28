from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Search_word").getOrCreate()

def search_word(word):
    df = spark.read.option('header', True).csv('/home/kimpass189/data/team_chat.csv')
    df_filtered = df.filter(col("message").contains("치킨"))
    df_filtered.show()
