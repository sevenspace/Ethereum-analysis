from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re


class topGas(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split())==3):
                fields=line.split()
                gasUsed=fields[1][1:-1]
                month=fields[2][1:-1]
                yield(month.replace('\"', ''), float(gasUsed))
        except:
            pass

    def reducer(self, month, val):
        total = 0
        total = sum(val)

        yield(month, total)


if __name__ == '__main__':
    #job3.JOBCONF= { 'mapreduce.job.reduces': '3' }
    topGas.run()
