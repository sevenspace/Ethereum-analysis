from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class top10(MRJob):

    def mapper(self, _, line):
        try:
            fields=line.split()
            if(len(fields)==5):
                address=fields[0].replace('\"', '')
                gasPrice=fields[1][1:-1]
                count = fields[2][0:-1]
                yield(None, (address, float(gasPrice), int(count)))
        except:
            pass

    def reducer(self, address, val):
        sorted_values = sorted(val, reverse=True, key=lambda x: x[2])
        #i =0
        for y in range (10):
            yield(address, sorted_values[y])


if __name__ == '__main__':
    top10.run()
