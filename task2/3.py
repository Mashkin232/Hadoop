from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[a-zA-Z]+")

class MaxFreqWord(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, count):
        yield None, (sum(count), word)

    def reducer(self, _, counts):
        yield max(counts)

if __name__ == '__main__':
    MaxFreqWord.run()

# 2985	"formula"