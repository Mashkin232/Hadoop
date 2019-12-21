from mrjob.job import MRJob
from mrjob.protocol import ReprProtocol
import re

WORD_RE = re.compile(r"[A-Za-zА-Яа-я][\w]+")

class CapitalCaseCount(MRJob):
    OUTPUT_PROTOCOL = ReprProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            if 'Я' >= word[0] >= 'А' or 'Z' >= word[0] >= 'A':
                yield word.lower(), True
            else:
                yield word.lower(), False

    def reducer(self, word, counts):
        upper_case_word_count = 0
        word_count = 0

        for i in counts:
            word_count += 1

            if i:
                upper_case_word_count += 1

        if word_count > 10 and upper_case_word_count > (word_count / 2):
            yield word, (upper_case_word_count, word_count)  

if __name__ == '__main__':
    CapitalCaseCount.run()