# 분석 코드 설명
## Hadoop상에서 구동되는 코드
### 1. 상위 30개 키워드 뽑기
MostFrequentWords.py

data는 hadoop hdfs:///user/maria_dev/BDP_final에 저장

==> python3.6 MostFrequentWords.py -r hadoop hdfs:///user/maria_dev/BDP_final/data_complete2.csv --output-dir hdfs:///user/maria_dev/BDP_final/output

결과를 hdfs:///user/maria_dev/BDP_final/output에 저장.

### 2. 댓글 뽑기
1) data_corlling_2.xlsx 파일을 로컬로 다운로드

==> hdfs dfs -get hdfs:///user/maria_dev/BDP_final/data_corlling_2.xlsx .

2) xlsx파일을 csv로 변환

python3.6 convert_excel_to_csv.py

==> data_corlling_2.csv 뱉어냄

3) pyspark 설치

sudo /usr/local/bin/python3.6 -m pip install pyspark==2.3.1


(버전 맞춰야함)

4) zshrc 설정

pyspark를 python3.6으로 실행되게 설정

export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6