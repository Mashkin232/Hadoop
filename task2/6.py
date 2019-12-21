from mrjob.job import MRJob
from mrjob.protocol import ReprProtocol
import re

WORD_RE = re.compile(r"[a-zа-я]\. ?[a-zа-я]\.")
threshold = 40

class FindAbbreviation(MRJob):
    OUTPUT_PROTOCOL = ReprProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield ''.join(word.split()), 1

    def reducer(self, word, count):
        word_count = sum(count)
        if word_count > threshold:
            yield word, word_count

if __name__ == '__main__':
    FindAbbreviation.run()
    
# "а.е."    70
# "в.д."    46
# "в.м."    68
# "в.н."    113
# "г.н."    63
# "г.р."    56
# "д.ч."    50
# "л.с."    79
# "н.э."    1117
# "с.ш."    117
# "т.д."    172
# "т.е."    138
# "т.н."    53
# "т.п."    82
# "ю.ш."    80