from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class scam11(MRJob):

    def mapper(self, _, line):
        try:
            field = line.split(",")
            if(len(line.split(","))==7):
                address = field[2]
                value = float(field[3])
                time_epoch = int(field[6])
                month = time.strftime("%Y-%m",time.gmtime(time_epoch)) #returns month of the year
                yield ((address,month), value)
        except:
            pass

    def reducer(self, address, val):
        total = 0
        total = sum(val)
        yield(address, total)

if __name__ == '__main__':
    scam11.JOBCONF= { 'mapreduce.job.reduces': '3' }
    scam11.run()
