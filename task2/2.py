from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class AvgWordLength(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield 1, len(word)

    def reducer(self, _, word_len):
        sum_length = 0
        word_count = 0

        for i in word_len:
            sum_length += i
            word_count += 1

        yield None, sum_length / word_count

if __name__ == '__main__':
    AvgWordLength.run()

# 6.057031234327786