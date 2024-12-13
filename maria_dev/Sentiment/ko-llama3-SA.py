from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, explode
from pyspark.sql.types import StringType
from langchain_community.chat_models import ChatOllama

# SparkSession 생성
spark = SparkSession.builder \
    .appName("CommentsSentimentAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020") \
    .getOrCreate()

# ChatOllama 모델 설정
llm = ChatOllama(
    model="llama3-Ko-Bllossom-8B",
    temperature=0,
     url="https://d33a-61-102-132-46.ngrok-free.app"
)

# 감정 분석 함수 정의
def analyze_sentiment(title, comment):
    try:
        if not comment:  # 댓글이 비어있는 경우 처리
            return "2"
        prompt = f"기사 제목: {title}\n 댓글: {comment}"
        response = llm.invoke(prompt)
        return response.content.strip()
    except Exception as e:
        print(f"Sentiment analysis failed for title: {title}, comment: {comment}, error: {e}")
        return "Error"

# HDFS에서 여러 CSV 파일 읽기
csv_paths = [
    f"hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/Result/comment{i}.csv"
    for i in range(1, 31)  # comment1.csv ~ comment30.csv
]
dataframes = [spark.read.csv(path, header=True, inferSchema=True) for path in csv_paths]

data = dataframes[0].withColumn("rank", lit(1))

# 나머지 데이터프레임을 처리하며 rank 컬럼 추가
for i, df in enumerate(dataframes[1:], start=2):
    df = df.withColumn("rank", lit(i))  # rank 값 추가
    data = data.union(df)  # union 연산

# Spark DataFrame을 Pandas DataFrame으로 변환
pandas_df = data.toPandas()

# 감정 분석 수행
pandas_df['Label'] = pandas_df.apply(lambda row: analyze_sentiment(row['Title'], row['Comment_Text']), axis=1)

# 결과를 다시 Spark DataFrame으로 변환
result_df = spark.createDataFrame(pandas_df)

# 결과 출력
result_df.show(truncate=False)

# 결과를 HDFS에 저장
result_df.write.mode("overwrite").csv(
    "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/sentiment_analysis/comments_sentiment",
    header=True
)
