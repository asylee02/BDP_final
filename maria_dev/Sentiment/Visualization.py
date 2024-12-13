import pandas as pd
from PIL import Image
import numpy as np
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import subprocess

# SparkSession 생성
spark = SparkSession.builder.appName("LoadSentimentData").getOrCreate()

# HDFS에서 저장된 CSV 파일 불러오기
hdfs_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/sentiment_analysis/comments_sentiment"
data_df = spark.read.csv(hdfs_path, header=True, inferSchema=True).toPandas()

# 워드클라우드 생성 함수 정의
def generate_wordcloud(data, image_path, output_path, label):
    # 마스크 이미지 로드
    icon = Image.open(image_path).convert("RGBA")
    new_img = Image.new("RGBA", icon.size, "WHITE")
    new_img.paste(icon, (0, 0), icon)
    mask = np.array(new_img.convert("RGB"))

    # 텍스트 데이터 결합
    text = " ".join(data)

    # 워드클라우드 생성
    wc = WordCloud(
        random_state=1234,
        font_path="malgun.ttf",
        width=2000,
        height=2000,
        background_color="white",
        mask=mask,
        colormap="inferno"
    ).generate(text)

    # 워드클라우드 저장
    plt.figure(figsize=(10, 10), dpi=300)
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.savefig(output_path, bbox_inches="tight", pad_inches=0)
    plt.close()

    # 저장된 이미지를 HDFS로 업로드
    hdfs_output_path = f"hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/sentiment_analysis/visualization/wordcloud_label_{label}.png"
    subprocess.run(["hdfs", "dfs", "-put", "-f", output_path, hdfs_output_path])
    print(f"Wordcloud for label {label} saved to HDFS at {hdfs_output_path}")

image_path = "pngegg.png"  

if "Comment_Text" in data_df.columns and "Label" in data_df.columns:
    for label in [0, 1, 2]:
        filtered_data = data_df[data_df["Label"] == label]["Comment_Text"].dropna()
        if not filtered_data.empty:
            output_path = f"wordcloud_label_{label}.png" 
            generate_wordcloud(filtered_data, image_path, output_path, label)
            print(f"Wordcloud for label {label} has been processed and added.")

else:
    print("Required columns 'Comment_Text' or 'Label' not found in the dataset.")
