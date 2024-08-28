from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import explode_outer, col, size

spark = SparkSession.builder.appName("team_chat").getOrCreate()
df=spark.read.option("header", "true").csv("/home/manggee/data/team_chat.csv")
dfc = df.createOrReplaceTempView("chat")
time_count = spark.sql("""
SELECT 
    SUM(CASE WHEN (time > '09:00:00' AND time < '10:00:00') THEN 1 ELSE 0 END) AS 9to10,
    SUM(CASE WHEN (time > '10:00:00' AND time < '11:00:00') THEN 1 ELSE 0 END) AS 10to11,
    SUM(CASE WHEN (time > '11:00:00' AND time < '12:00:00') THEN 1 ELSE 0 END) AS 11to12,
    SUM(CASE WHEN (time > '12:00:00' AND time < '13:00:00') THEN 1 ELSE 0 END) AS 12to1,
    SUM(CASE WHEN (time > '13:00:00' AND time < '14:00:00') THEN 1 ELSE 0 END) AS 1to2,
    SUM(CASE WHEN (time > '14:00:00' AND time < '15:00:00') THEN 1 ELSE 0 END) AS 2to3,
    SUM(CASE WHEN (time > '15:00:00' AND time < '16:00:00') THEN 1 ELSE 0 END) AS 3to4,
    SUM(CASE WHEN (time > '16:00:00' AND time < '17:00:00') THEN 1 ELSE 0 END) AS 4to5,
    SUM(CASE WHEN (time > '17:00:00' AND time < '18:00:00') THEN 1 ELSE 0 END) AS 5to6
FROM 
    chat;
""")
time_count.createOrReplaceTempView("time_count")
words_df = spark.sql("""
SELECT word, COUNT(*) AS count
FROM (
    SELECT explode(split(message, ' ')) AS word
    FROM chat
) exploded_words
WHERE word <> ''
GROUP BY word
ORDER BY count DESC
LIMIT 10
""")

words_df.createOrReplaceTempView("words")
words_df.show()
user_count = spark.sql("""
SELECT sender, COUNT(*) AS count_user
FROM chat
GROUP BY sender
ORDER BY count_user desc 
""")
user_count.createOrReplaceTempView("user_count")


time_count.show()
user_count.show()
spark = SparkSession.builder.appName("team_chat").getOrCreate()
#rdf.write.mode("overwrite").parquet('/home/manggee/data/')

spark.stop()
