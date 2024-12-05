from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol  # �^��^�
import re

WORD_RE = re.compile(r"[\w']+")


class MostFrequentWords(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol  # RawValueProtocol�^�^� �^¬�^ک�^�^��^׬ plain text �^�력

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_30)
        ]

    def mapper_get_words(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) > 1:  # �^��^�^��^�^� 2�^� �^ݴ�^�^��^�^� 컬�^߼�^ݴ �^�^��^�^� �^�^��^� �^�리
                title = fields[1]  # Title 컬�^߼
                for word in WORD_RE.findall(title):
                    yield word.lower(), 1
        except Exception:
            pass  # �^�^�못�^�^� �^Ͱ�^ݴ�^İ�^�^� 무�^�^�

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)  # �^˨�^ִ �^��^�^� �^��^° �^�^� �^��^ˬ

    def reducer_find_top_30(self, _, word_count_pairs):
        top_30 = sorted(word_count_pairs, reverse=True, key=lambda x: x[0])[:30]
        for count, word in top_30:
            try:
                decoded_word = word.encode('latin1').decode('utf-8')  # �^ܠ�^�^��^��^�^� �^�^��^��^ԩ
            except (UnicodeEncodeError, UnicodeDecodeError):
                decoded_word = word  # �^�^��^��^ԩ �^ˤ�^̨ �^�^� �^�^�본 �^��^�^�
            yield None, f"{decoded_word}\t{count}"  # plain text �^�^��^�^��^ܼ�^� �^�력


if __name__ == '__main__':
    MostFrequentWords.run()
