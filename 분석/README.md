# 분석 코드 설명
## Hadoop상에서 구동되는 코드
### 1. 상위 30개 키워드 뽑기
데이터부터 csv로 변경

MostFrequentWords.py

data는 hadoop hdfs:///user/maria_dev/BDP_final에 저장

==> python3.6 MostFrequentWords.py -r hadoop hdfs:///user/maria_dev/BDP_final/data_complete2.csv --output-dir hdfs:///user/maria_dev/BDP_final/output

* 서버에서는 python3.6 MostFrequentWords.py -r hadoop hdfs:///user/maria_dev/preprocessing/data_complete.csv --output-dir hdfs:///user/maria_dev/BDP_final/output

결과를 hdfs:///user/maria_dev/BDP_final/output에 저장.

### 2. 30개 키워드 전처리
preprocessing_output.py를 실행해서 키워드에서 필요없는 언급수 빼기

파일은 '/user/maria_dev/BDP_final/output/keywords_only.txt'에 저장

### 3. 기사 데이터 전처리
preprocessing_csv2.py를 실행해서 기사를 전처리

제목(Title), 댓글(Comment_Text), 좋아요수(Comment_value)로 전처리

파일은 '/user/maria_dev/BDP_final/parsed_comments_output.csv'에 저장

### 4 filtering csv 실행
processed_data.csv 저장


### 5 JSON 코멘트 파싱
processed_data.csv 이용해서 코멘트 parsing

기사 제목과 댓글만 남겨놓음

parsed_comments_output.csv 저장

* 서버에서는 user/maria_dev/BDP_final/parsed_comments_output.csv/merged_output.csv로 저장됨

### 6. 키워드별로 댓글 필터링
filtering_pyspark.py 실행

키워드, 댓글, 좋아요수로 필터링

파일은 '/user/maria_dev/BDP_final/keyword_comments' 디렉토리 안에 저장

* 서버에서는 merged_output.csv와 keywords_only.txt로 코멘트 뽑아냄
