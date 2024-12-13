from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
import os
from openai import OpenAI

# SparkSession 생성
spark = SparkSession.builder \
    .appName("CommentsSentimentAnalysisWithGPT") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020") \
    .getOrCreate()

# OpenAI API 설정
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# System Prompt 설정
system_prompt = """
당신은 뉴스 기사의 제목과 그에 대한 댓글의 감정을 정확하게 분류하는 전문가입니다. 주어진 댓글을 신중히 분석하여 그 감정을 판단하세요.

다음 지침을 따라주세요:
1. 댓글이 전반적으로 긍정적이면 '1'을 출력하세요.
2. 댓글이 전반적으로 부정적이거나 중요한 부정적 요소를 포함하고 있다면 '0'을 출력하세요.
3. 댓글이 긍정도 부정도 아닌 중립적인 의견일 경우 '2'를 출력하세요.
4. 반드시 '1', '0', 또는 '2'만을 출력하고, 다른 텍스트는 포함하지 마세요.

분류 시 다음 사항을 고려하세요:
- 댓글의 전반적인 톤과 감정을 주의 깊게 파악하세요.
- 긍정적인 표현과 부정적인 표현의 비중을 신중히 비교하세요.
- 뉴스 제목의 내용과 댓글의 관련성을 고려하세요.
- 정치적 성향이나 개인적 견해에 치우치지 않고 객관적으로 판단하세요.

부정 댓글 판단 시 특히 주의해야 할 점:
- 비난, 비판, 불만, 분노 등의 부정적 감정이 표현되었는지 확인하세요.
- 비속어, 욕설, 모욕적인 표현이 사용되었는지 살펴보세요.
- 뉴스 내용이나 관련 인물에 대한 강한 반대 의견이 있는지 주목하세요.
- 풍자나 비꼼의 어조가 사용되었는지 확인하세요.

긍정 댓글 판단 시 특히 주의해야 할 점:
- 지지, 동의, 칭찬 등의 긍정적 감정이 표현되었는지 확인하세요.
- 뉴스 내용이나 관련 인물에 대한 옹호나 긍정적 평가가 있는지 살펴보세요.
- 건설적인 제안이나 희망적인 전망이 제시되었는지 주목하세요.

중립 댓글 판단 시 특히 주의해야 할 점:
- 감정적인 표현이 거의 없고 단순한 정보 전달이나 의견 공유에 그치는지 확인하세요.
- 긍정과 부정이 균형을 이루어 명확히 어느 쪽으로도 치우치지 않는 경우로 판단하세요.

댓글을 신중히 분석하고, 균형 잡힌 판단을 내려주세요. 부정적인 요소에 특히 주의를 기울이되, 전체적인 맥락을 고려하여 분류해주세요.
"""

# LLM 호출 함수
def llm(title, comment):
    message = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Title: {title}, Comment: {comment}"}
    ]
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=message
        )
        answer = response.choices[0].message.content.strip()
        if answer in ['1', '0', '2']:
            return answer
        else:
            return "Invalid Response"
    except Exception as e:
        print(f"OpenAI API Error: {e}")
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
pandas_df['Label'] = pandas_df.apply(lambda row: llm(row['Title'], row['Comment_Text']), axis=1)

# 결과를 다시 Spark DataFrame으로 변환
result_df = spark.createDataFrame(pandas_df)

# 결과 출력
result_df.show(truncate=False)

# 결과를 HDFS에 저장
result_df.write.mode("overwrite").csv(
    "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/sentiment_analysis/comments_sentiment",
    header=True
)