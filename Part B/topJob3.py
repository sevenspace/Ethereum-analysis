from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class job3(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split())==2):
                fields=line.split()
                address=fields[0]
                value=fields[1].replace('\"', '')
                yield(None, (address.replace('\"', ''), float(value)))
        except:
            pass

    def reducer(self, address, val):
        sorted_values = sorted(val, reverse=True, key=lambda x: x[1])
        #i =0
        for y in range (10):
            yield(address, sorted_values[y])


if __name__ == '__main__':
    #job3.JOBCONF= { 'mapreduce.job.reduces': '3' }
    job3.run()
