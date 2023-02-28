from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class gasTime(MRJob):

    SORT_VALUES = True

    def mapper(self, _, line):
        try:
            if(len(line.split(','))==9):
                fields=line.split(',')
                join_key=fields[0]
                gasUsed = int(fields[6])
                time_epoch = int(fields[7])
                month = time.strftime("%Y-%m",time.gmtime(time_epoch)) #returns month of the year
                yield (join_key,(2, gasUsed, month))

            elif(len(line.split(','))==5):
                fields=line.split(',')
                join_key=fields[3]
                count = 1
                yield (join_key,(1, None))

        except:
            pass

    def reducer(self, block, val):
        values = list(val)
        contract =[]

        for value in values:
            if value[0]==2:
                if block in contract:
                    yield (block, (value[1], value[2]))
            elif value[0]==1:
                contract.append(block)


if __name__ == '__main__':
    gasTime.JOBCONF= { 'mapreduce.job.reduces': '3' }
    gasTime.run()
