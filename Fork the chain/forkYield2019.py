from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class fork(MRJob):

    SORT_VALUES = True

    def mapper(self, _, line):
        try:
            if(len(line.split(','))==7):
                fields=line.split(',')
                gasPrice = int(fields[5])
                time_epoch = int(fields[6])
                receiver = fields[2]
                month = time.strftime("%m",time.gmtime(time_epoch)) #returns month of the year
                year = time.strftime("%Y",time.gmtime(time_epoch))
                if year == "2019" and month == "02":
                    #time = (year + "-" + month)
                    yield (receiver, (gasPrice, 1, year, month))

        except:
            pass
    # #
    def reducer(self, address, val):
        count = 0
        total= 0
        year = ""
        month = ""
        average = 0

        for value in val:
            total = total + value[0]
            count = count + value[1]
            year = value[2]
            month = value[3]
        average = total/count
        yield(address, (average, count, year, month))

if __name__ == '__main__':
    fork.JOBCONF= { 'mapreduce.job.reduces': '3' }
    fork.run()
