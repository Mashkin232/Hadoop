from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w]+")

class MaxLengthWord(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, [len(word), word.lower()]


    def reducer(self, _, word_len):
        yield max(word_len)

if __name__ == '__main__':
    MaxLengthWord.run()

# 63 "rindfleischetikettierungs\u00fcberwachungsaufgaben\u00fcbertragungsgesetz"