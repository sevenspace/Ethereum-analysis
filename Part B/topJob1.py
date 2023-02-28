from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re


class job1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields)==7):
                address = fields[2]
                value = fields[3]
                yield (address, float(value))
        except:
            pass
            #no need to do anything, just ignore the line, as it was malformed

    def reducer(self, address, value):
        total = sum(value)
        yield(address, total)

if __name__ == '__main__':
    job1.JOBCONF= { 'mapreduce.job.reduces': '3' }
    job1.run()
