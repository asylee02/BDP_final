import csv
import asyncio
from playwright.async_api import async_playwright

# CSV 파일 저장 함수
def save_to_csv(politics_list, output_file):
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Index", "Title", "Date", "Total Comments", "Comments"])
        for idx, (title, datetime_text, comment_count, comments_with_likes) in enumerate(politics_list, 1):
            formatted_comments = str(comments_with_likes)
            writer.writerow([idx, title, datetime_text, comment_count, formatted_comments])

# 메인 작업 함수
async def main():
    url = "https://news.naver.com/section/100"
    politics_list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # 브라우저 실행 (headless 모드)
        page = await browser.new_page()

        # URL 이동
        await page.goto(url)

        # "더보기" 버튼 클릭 반복
        while True:
            try:
                # "더보기" 버튼 클릭
                await page.click(".section_more_inner._CONTENT_LIST_LOAD_MORE_BUTTON", timeout=3000)
                await page.wait_for_timeout(1000)  # 로딩 대기
                print("Button clicked, loading more content...")
            except:
                print("No more '더보기' button found or error occurred!")
                break

        # 기사 데이터 추출
        articles_all = await page.locator("div.sa_text").all()

        for article_all in articles_all:
            # 댓글 수 확인
            comment_count_element = await article_all.locator("a.sa_text_cmt._COMMENT_COUNT_LIST").text_content()
            if not comment_count_element or "댓글" not in comment_count_element:
                print("아예 댓글 없음")
                continue

            comment_count = int(comment_count_element.split("댓글")[0].replace(",", ""))
            if comment_count < 9:
                print(f"댓글 개수 부족: {comment_count}")
                continue

            # 제목과 링크 추출
            article = await article_all.locator("a.sa_text_title._NLOG_IMPRESSION").first
            href = await article.get_attribute("href")
            title = await article.text_content()

            # 게시 날짜 추출
            datetime_element = await article_all.locator("div.sa_text_datetime").text_content()
            datetime_text = datetime_element.strip() if datetime_element else "날짜 정보 없음"

            # 절대 URL 생성
            full_url = "https://news.naver.com" + href if href.startswith("/") else href

            # 기사 페이지로 이동
            try:
                await page.goto(full_url)
                await page.wait_for_timeout(2000)  # 페이지 로딩 대기

                # "댓글 보기" 버튼 클릭
                try:
                    await page.click(".u_cbox_btn_view_comment", timeout=5000)
                    await page.wait_for_timeout(2000)  # 댓글 페이지 로딩 대기
                except:
                    print("Failed to click comment view button!")
                    continue

                # 댓글 추출
                comments_with_likes = []
                comment_elements = await page.locator(".u_cbox_contents").all()
                for comment_element in comment_elements:
                    comment_text = await comment_element.text_content()

                    # "클린봇" 댓글 필터링
                    if "클린봇이 부적절한 표현을 감지한 댓글입니다." in comment_text:
                        continue

                    # 좋아요 수 추출
                    like_element = await comment_element.locator("xpath=../following-sibling::div//em[@class='u_cbox_cnt_recomm']").text_content()
                    like_count = int(like_element.replace(",", "")) if like_element.isdigit() else 0

                    comments_with_likes.append([comment_text, like_count])

                # 기사 정보 저장
                politics_list.append((title.strip(), datetime_text, comment_count, comments_with_likes))
            except Exception as e:
                print(f"Failed to load page: {full_url}, Error: {e}")

        await browser.close()

    # CSV 저장
    output_file = "politics_articles_comments_with_likes.csv"
    save_to_csv(politics_list, output_file)
    print(f"Data saved to {output_file}")

# asyncio 실행
asyncio.run(main())
