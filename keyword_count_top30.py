
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import csv
import sys
from heapq import nlargest

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

class KeywordCountTop30(MRJob):
    def steps(self):
        return [
              MRStep(
                    mapper=self.map_words,
                    combiner=self.combine_words,
                    reducer=self.reduce_words
                    ),
              MRStep(
                  reducer=self.find_top_30
              )

              ]
    def map_words(self, _, line):
        reader = csv.reader([line])
        for fields in reader:
            if fields[0] != 'Index':
                title = fields[1].strip()[1:-1]
                try:
                    words = ast.literal_eval(title)
                    for word in words:
                        yield (word, 1)
                except SyntaxError as se:
                    yield ("SYNTAX_ERROR", title)
                except Exception as e:
                    yield ("DECODING_ERROR", f"{title}: {e}")
    def combine_words(self, word, counts):
        yield (word, sum(counts))
    def reduce_words(self, word, counts):
        yield None, (word, sum(counts))
    def find_top_30(self, _, word_counts):
        top_30 = nlargest(30, word_counts, key=lambda x: x[1])
        for word, count in top_30:
            yield word, count
if __name__ == '__main__':
    KeywordCountTop30.run()
