import json
import time
import pandas as pd  # pandas 라이브러리 추가
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# 크롤링 함수
def crawl_articles(output_file="articles.json", excel_file="articles.xlsx"):
    url = "https://news.naver.com/section/100"

    # Firefox 사용
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")  # Headless 모드 사용
    driver = webdriver.Firefox(options=options)
    driver.get(url)

    politics_list = []  # 기사 데이터를 저장할 리스트

    WebDriverWait(driver, 10).until(
      EC.presence_of_element_located((By.CSS_SELECTOR, "div.sa_text"))
    )
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    articles_all = soup.select("div.sa_text")

    # print(articles_all)
    for article_all in articles_all:

        comment_count_element = article_all.find_next("a", class_="sa_text_cmt _COMMENT_COUNT_LIST")
            

        article = article_all.find("a", class_="sa_text_title _NLOG_IMPRESSION")


        href = article.get("href")
        title = article.get_text(strip=True)

        datetime_element = article_all.find_next("div", class_="sa_text_datetime")
        datetime_text = datetime_element.get_text(strip=True) if datetime_element else "날짜 정보 없음"

        full_url = "https://news.naver.com" + href if href.startswith("/") else href
        driver.get(full_url)
        html = driver.page_source
        print(html)
        try:
            driver.get(full_url)
            time.sleep(2)

            try:
                view_comment_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "u_cbox_btn_view_comment"))
                )
                view_comment_button.click()
                time.sleep(2)
            except:
                continue

            comments_with_likes = []
            comment_elements = driver.find_elements(By.CSS_SELECTOR, ".u_cbox_contents")

            for comment_element in comment_elements:
                comment_text = comment_element.text

                if "클린봇이 부적절한 표현을 감지한 댓글입니다." in comment_text:
                    continue

                like_element = comment_element.find_element(By.XPATH, "../following-sibling::div//em[@class='u_cbox_cnt_recomm']")
                like_count = int(like_element.text.replace(",", "")) if like_element.text.isdigit() else 0

                comments_with_likes.append({"comment": comment_text, "likes": like_count})

            politics_list.append({
                "title": title,
                "date": datetime_text,
                "total_comments": comment_count,
                "comments": comments_with_likes
            })

        except:
            continue
    driver.quit()

    # Excel 파일로 저장
    df = pd.DataFrame([
        {
            "Title": article["title"],
            "Date": article["date"],
            "Total Comments": article["total_comments"],
            "Comments": json.dumps(article["comments"], ensure_ascii=False)
        }
        for article in politics_list
    ])
    df.to_excel(excel_file, index=False)  # 'encoding' 파라미터 제거
    print(f"Data saved to {excel_file}")

if __name__ == "__main__":
    crawl_articles()
