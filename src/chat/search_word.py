from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#spark = SparkSession.builder.appName("Search_word").getOrCreate()
def search_word(word):
    # Spark 세션 생성
    spark = SparkSession.builder.appName("Search_word").getOrCreate()
    
    # CSV 파일 읽기
    df = spark.read.option('header', True).csv('/home/kimpass189/data/team_chat.csv')
    
    # 메시지 컬럼에서 단어를 포함하는 행 필터링
    df_filtered = df.filter(col("message").contains(word))
    
    # 결과를 문자열로 변환하여 반환
    result = df_filtered.collect()  # 필터링된 DataFrame을 리스트로 변환
    result_str = "\n".join([f"{row['message']}" for row in result])  # 각 행을 문자열로 변환하여 결합
    
    return result_str if result_str else "No matching messages found."
