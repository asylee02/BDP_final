from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col
from pyspark.sql.types import StringType, StructType, StructField, ArrayType

import ast
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
def analyze_sentiment(comment):
    try:
        if not comment:  # 댓글이 비어있는 경우 처리
            return "중립"
        prompt = f"댓글의 감정을 무조건 두글자로 분석해주세요: '{comment}'"
        response = llm.invoke(prompt)
        return response.content.strip()
    except Exception as e:
        print(f"Sentiment analysis failed for comment: {comment}, error: {e}")
        return "Error"

# 감정 분석을 UDF로 등록
analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# 문자열 형태의 Comments를 리스트로 변환
def parse_comments(comment_data):
    try:
        return ast.literal_eval(comment_data) if comment_data else []
    except Exception as e:
        print(f"Failed to parse comments: {comment_data}, error: {e}")
        return []

# UDF로 등록
parse_comments_udf = udf(parse_comments, ArrayType(StructType([
    StructField("comment", StringType(), True),
    StructField("like", StringType(), True)
])))

# HDFS에서 여러 CSV 파일 읽기
csv_paths = [
    "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/mapreduce/comment1.csv",
    # 추가 파일 경로를 여기에 추가
]
dataframes = [spark.read.csv(path, header=True, inferSchema=True) for path in csv_paths]

# CSV 파일들을 하나의 DataFrame으로 합치기
data = dataframes[0]
for df in dataframes[1:]:
    data = data.union(df)

# Comments 열을 파싱하여 리스트로 변환
data = data.withColumn("Comments", parse_comments_udf(col("Comments")))

# 리스트를 개별 행으로 분리
data = data.withColumn("CommentsExploded", explode(col("Comments")))

# Comments와 Likes 분리
data = data.withColumn("Comment", col("CommentsExploded.comment")) \
           .withColumn("Likes", col("CommentsExploded.like")) \
           .drop("CommentsExploded", "Comments")

# 감정 분석 열 추가
data = data.withColumn("Sentiment", analyze_sentiment_udf(col("Comment")))

# 결과 출력
data.show(truncate=False)

# 결과를 HDFS에 저장
data.write.mode("overwrite").csv(
    "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/sentiment_analysis/comments_sentiment",
    header=True
)
