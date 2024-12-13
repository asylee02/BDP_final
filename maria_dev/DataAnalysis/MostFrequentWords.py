from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re

WORD_RE = re.compile(r"[\w']+")


class MostFrequentWords(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol  # RawValueProtocol를 사용하여 plain text 출력
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_30)
        ]

    def mapper_get_words(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) > 1:  # 최소한 2개 이상의 컬럼이 있을 때만 처리
                title = fields[1]  # Title 컬럼
                for word in WORD_RE.findall(title):
                    yield word.lower(), 1
        except Exception:
            pass  # 잘못된 데이터는 무시

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)  # 단어 빈도 계산 후 전달

    def reducer_find_top_30(self, _, word_count_pairs):
        top_30 = sorted(word_count_pairs, reverse=True, key=lambda x: x[0])[:30]
        for count, word in top_30:
            try:
                decoded_word = word.encode('latin1').decode('utf-8')  # 유니코드 디코딩
            except (UnicodeEncodeError, UnicodeDecodeError):
                decoded_word = word  # 디코딩 실패시 원본 반환
            yield None, f"{decoded_word}\t{count}"  # plain text 형식으로 출력


if __name__ == '__main__':
    MostFrequentWords.run()