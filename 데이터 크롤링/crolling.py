import csv
from selenium import webdriver as wb
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
import time

# URL 설정
url = "https://news.naver.com/section/100"

# WebDriver 초기화
driver = wb.Chrome()

# URL로 이동
driver.get(url)

# "더보기" 버튼 클릭 반복
while True:
    try:
        # "더보기" 버튼 대기
        search = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".section_more_inner._CONTENT_LIST_LOAD_MORE_BUTTON"))
        )

        # 페이지 HTML 가져오기
        html = driver.page_source
        soup = bs(html, "html.parser")

        # 모든 기사의 날짜 확인


        # "더보기" 버튼 클릭
        search.click()
        print("Button clicked, loading more content...")
        time.sleep(1)  # 로딩 대기
    except:
        print("No more '더보기' button found or error occurred!")
        break

# 다시 페이지 HTML 가져오기
html = driver.page_source
soup = bs(html, "html.parser")

# a 태그에서 href와 제목 추출
articles_all = soup.select("div.sa_text")
politics_list = []  # 정치 기사 제목과 댓글 저장할 리스트

for article_all in articles_all:
    # 댓글 수 확인
    comment_count_element = article_all.find_next("a", class_="sa_text_cmt _COMMENT_COUNT_LIST")
    if not comment_count_element or not comment_count_element.get_text(strip=True):
        print('아예 댓글 없음')
        continue

    comment_count = int(comment_count_element.get_text(strip=True)[:-3].replace(",", ""))  # 댓글 개수 추출
    if comment_count < 9:
        print(f"댓글 개수 부족: {comment_count}")
        continue

    article = article_all.find("a", class_="sa_text_title _NLOG_IMPRESSION")
    if not article:
        continue

    href = article.get("href")  # href 속성 값
    title = article.get_text(strip=True)  # 제목 텍스트

    # 게시 날짜 확인
    datetime_element = article_all.find_next("div", class_="sa_text_datetime")
    if datetime_element:
        datetime_text = datetime_element.get_text(strip=True)
    else:
        datetime_text = "날짜 정보 없음"

    # 절대 경로 생성
    full_url = "https://news.naver.com" + href if href.startswith("/") else href

    # 해당 링크로 이동
    try:
        driver.get(full_url)
        time.sleep(2)  # 페이지 로딩 대기

        # 댓글 추출 (더보기 버튼 클릭 없이 현재 로드된 댓글만 가져옴)
        comment_elements = driver.find_elements(By.CSS_SELECTOR, ".u_cbox_comment")
        comments_with_likes = []
        
        for comment_element in comment_elements:
            comment_text = comment_element.find_element(By.CSS_SELECTOR, ".u_cbox_text_wrap").text
            like_element = comment_element.find_element(By.CSS_SELECTOR, "em.u_cbox_cnt_recomm")
            like_count = int(like_element.text.replace(",", "")) if like_element.text.isdigit() else 0
            comments_with_likes.append([comment_text, like_count])

            
        # 기사 제목과 댓글, 좋아요 저장
        politics_list.append((title, comments_with_likes))
    except Exception as e:
        print(f"Failed to load page: {full_url}, Error: {e}")

# 브라우저 종료
driver.quit()

# CSV 파일로 저장
output_file = "politics_articles_comments_with_likes.csv"
with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    
    # 헤더 작성
    writer.writerow(["Index", "Title", "Comments"])
    
    # 데이터 작성
    for idx, (title, comments_with_likes) in enumerate(politics_list, 1):
        # 댓글과 좋아요를 대괄호 포함된 2차원 배열 형식으로 변환
        formatted_comments = str(comments_with_likes)
        writer.writerow([idx, title, formatted_comments])

print(f"Data saved to {output_file}")
