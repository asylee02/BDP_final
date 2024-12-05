from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re

WORD_RE = re.compile(r"[\w']+")

class MostFrequentWords(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol


    def steps(self):
        return [
                MRStep(mapper=self.mapper_filter_comments,
                       reducer=self.reducer_count_words),
                MRStep(reducer=self.reducer_find_top_30)
            ]
    def mapper_filter_comments(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) > 3:
                comments = int(fields[-1].strip())
                if comments >= 300:
                    title = fields[1]
                    for word in WORD_RE.findall(title):
                        yield word.lower(), 1
        except Exception:
            pass
    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)
    def reducer_find_top_30(self, _, word_count_pairs):
        top_30 = sorted(word_count_pairs, reverse=True, key=lambda x: x[0])[:30]
        for count, word in top_30:
            try:
                decoded_word = word.encode('latin1').decode('utf-8')
            except (UnicodeEncodeError, UnicodeDecodeError):
                decoded_word = word
            yield None, f"{decoded_word}\t{count}"
if __name__ == '__main__':
    MostFrequentWords.run()

