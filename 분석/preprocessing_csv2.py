import pandas as pd
import ast
from pyspark.sql import SparkSession

# Spark Session 생성
spark = SparkSession.builder \
    .appName("Process and Upload Comments Data") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020") \
    .getOrCreate()

# HDFS에서 파일 읽기
hdfs_input_path = "hdfs:///user/maria_dev/BDP_final/processed_data.csv"
hdfs_output_path = "hdfs:///user/maria_dev/BDP_final/parsed_comments_output"
single_file_output_path = "hdfs:///user/maria_dev/BDP_final/parsed_comments_output.csv"

# HDFS에서 데이터 읽기
df = spark.read.csv(hdfs_input_path, header=True, inferSchema=True)

# Spark DataFrame을 Pandas DataFrame으로 변환
data = df.toPandas()

# 개행 문자 포함된 Title 제거
data['Title'] = data['Title'].str.strip()
data['Title'] = data['Title'].str.replace(r'\s+', ' ', regex=True)  # 공백 정리
data = data[~data['Title'].str.contains(r'\n', na=False)]  # 개행 문자 제거

# 큰따옴표 또는 이스케이프 처리된 큰따옴표 포함된 Title 제거
data = data[~data['Title'].str.contains(r'"|\\\"', na=False)]

# JSON 파싱 함수 정의
def safe_parse_comments(comment):
    """
    Parse the JSON-like string into a structured format.
    Handles exceptions and malformed data.
    """
    try:
        # 작은따옴표를 큰따옴표로 변환하여 JSON 표준에 맞춤
        if isinstance(comment, str):
            comment = comment.replace("'", '"')
        parsed = ast.literal_eval(comment)
        if isinstance(parsed, list):
            # 리스트 내부의 요소도 검증
            return [item for item in parsed if isinstance(item, list) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], (int, float))]
        return None  # 리스트가 아니면 None 반환
    except Exception:
        return None  # 파싱 실패 시 None 반환

# Comments 컬럼 파싱 적용
data['Parsed_Comments'] = data['Comments'].apply(safe_parse_comments)

# NaN 데이터와 비정형 데이터 제거
data = data.dropna(subset=['Parsed_Comments'])  # Parsed_Comments가 비어 있는 경우 제거

# Parsed_Comments 열을 펼침
data = data.explode('Parsed_Comments')

# 텍스트와 값 추출 함수 정의
def extract_comment_details(comment):
    """
    Extract text and value from a comment, handling malformed data.
    """
    if isinstance(comment, list) and len(comment) == 2:
        return comment[0], comment[1]
    return None, None

# Comment_Text와 Comment_Value 추출
data['Comment_Text'], data['Comment_Value'] = zip(*data['Parsed_Comments'].apply(extract_comment_details))

# 잘못된 행 제거 (Comment_Text나 Comment_Value가 없는 경우)
data = data.dropna(subset=['Comment_Text', 'Comment_Value'])

# 불필요한 컬럼 제거
data_cleaned = data.drop(columns=['Parsed_Comments', 'Comments'])

# Pandas DataFrame을 Spark DataFrame으로 변환
spark_cleaned_df = spark.createDataFrame(data_cleaned)

# 데이터를 단일 파티션으로 병합하여 저장
temp_output_path = "hdfs:///user/maria_dev/BDP_final/temp_parsed_comments_output"
spark_cleaned_df.coalesce(1).write.mode("overwrite").csv(temp_output_path, header=True)

# 병합된 파일을 하나의 CSV로 저장
import subprocess

# 로컬로 병합된 파일 가져오기
subprocess.run(["hdfs", "dfs", "-getmerge", temp_output_path, "/tmp/merged_output.csv"])

# 병합된 파일을 다시 HDFS에 업로드
subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/merged_output.csv", single_file_output_path])

print(f"단일 파일로 병합된 데이터가 {single_file_output_path}에 저장되었습니다.")