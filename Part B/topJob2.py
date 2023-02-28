from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class job2(MRJob):

    SORT_VALUES = True

    def mapper(self, _, line):
        try:
            if(len(line.split())==2):
                fields=line.split()
                join_key=fields[0]
                join_value=fields[1]
                yield (join_key.replace('\"', ''),(2, join_value))

            elif(len(line.split(','))==5):
                fields=line.split(',')
                join_key=fields[0]
                yield (join_key,(1, None))
        except:
            pass

    def reducer(self, address, val):
        values = list(val)
        contract =[]

        for value in values:
            if value[0]==2:
                if address in contract:
                    yield (address, value[1])
            elif value[0]==1:
                #inAddress = True
                contract.append(address)

        #if join_value != 0 and inAddress != False:
        #    yield(address, join_value)

if __name__ == '__main__':
    job2.JOBCONF= { 'mapreduce.job.reduces': '3' }
    job2.run()
