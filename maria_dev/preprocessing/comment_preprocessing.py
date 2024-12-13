import pandas as pd
import matplotlib.pyplot as plt
import ast  # 문자열로 저장된 리스트를 파싱
import re # 불용어 처리
from konlpy.tag import Hannanum, Mecab # 토큰화

def clean(text):
    text_lower = text.lower() # 영어 소문자로 통일
    text_clean = re.sub(r"[^a-z|가-힣]", " ", text_lower) # 특수 문자 등 제거 # \"' 따옴표 살리기
    return text_clean

def re_stopwords(tokens, stopwords):
    filtered_tokens = [t for t in tokens if t not in stopwords] # nouns로 토큰화 
    return filtered_tokens

def re_one_word(tokens): # 한 글자인 토큰 제거
    return [token for token in tokens if len(token)>1] 

def main():
    file_path = "/home/maria_dev/preprocessing/data_complete2.csv" # 파일 위치 지정 
    df_new = pd.read_csv(file_path)
    df_new["Title"] = df_new["Title"].apply(ast.literal_eval)
    df_new['Comments'] = df_new['Comments'].apply(ast.literal_eval)

    top = pd.read_csv("/home/maria_dev/mapreduce/output.txt", sep="\t", encoding='utf-8', header=None)
    top[0] = top[0].str.replace("'", "", regex=False) # top30 파일
    top = top[top[1]>10] # top30으로 했더니 기사 2800개 중 2100개가 선택됨 -> count 10개 이상인 것만  
    top_li = list(top[0])

    df_top = df_new[df_new["Title"].apply(lambda x: any(word in x for word in top_li))] # 필터링

    comments_li = [] # Comments를 따로 df로 만들기
    for idx, row in df_top.iterrows():
        for comment in row['Comments']:
            comments_li.append({
                "Index": row['Index'],  # 원본 Index
                "Title": row['Title'],
                "Comment": comment[0],  # 댓글 텍스트
                "Likes": comment[1]     # 댓글 좋아요 수
            })
    df_top_comments = pd.DataFrame(comments_li) # Comments df

    df_comments = df_top_comments.copy()

    df_comments['Comment'] = df_comments['Comment'].apply(clean)

    mecab = Mecab() 

    df_comments_token = df_comments.copy()
    df_comments_token['Comment'] = df_comments_token['Comment'].apply(lambda x: mecab.nouns(x)) # nouns

    stopwords = """
    것 때문 때 시각 무렵 시간 전 후 이유 까닭 경우 중 내 외 근거 측면 사람 측 차원 결과 앞 뒤 사실 주장 생각 의견 가운데 관련 문제 의혹 지적 부분 모두 정도 대부분 당시 현재 여부 우리 저희 제 나 저 이것 그것 저것 여기 거기 저기
    의해 기준 이해 확신 주장 가속 상식 시간 속보 부분 사과 의원 기자 명백 다음 시절 행사 존중 깔끔 표명 무게 안정 강력 특보 단독 책임 
    것 등 및 말 수 바 전 후 더 때 중 내 듯 때문 또 로 면 몇 아래 위 저 나 우리 저희 당 측 종합 단도직입 르포 팩트체크 전문 현장 집중취재 긴급 뉴스 영상 포토 논란 의혹 파장 입장 결과 사태 정황 분위기 반응 여부 문제 관련 상황 전망 가능성 이어 따라 대해 통해 의해 위해
    """
    stopwords_list = stopwords.split()

    df_comments_stop = df_comments_token.copy()
    df_comments_stop['Comment'] = df_comments_stop['Comment'].apply(lambda x: re_stopwords(x, stopwords_list))
    df_comments_stop['Comment'] = df_comments_stop['Comment'].apply(lambda x: re_one_word(x))

    df_comments_stop.to_csv("/home/maria_dev/preprocessing/comments_complete.csv", encoding="utf-8",index=False)

if __name__ == "__main__":
    main()
