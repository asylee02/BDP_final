#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import csv
import sys

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

class KeywordCount(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.map_words,
                combiner=self.combine_words,
                reducer=self.reduce_words
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
        yield (word, sum(counts))


if __name__ == '__main__':
    KeywordCount.run()















