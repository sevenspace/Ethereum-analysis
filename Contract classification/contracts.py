from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class contracts(MRJob):

    SORT_VALUES = True

    def mapper(self, _, line):
        try:
            if(len(line.split(','))==7):
                fields=line.split(',')
                join_key=fields[2]
                gasPrice = int(fields[5])
                value = float(fields[3])
                yield (join_key,(2, value, gasPrice))

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
                    yield (address, (value[1], value[2]))
            elif value[0]==1:
                contract.append(address)


if __name__ == '__main__':
    contracts.JOBCONF= { 'mapreduce.job.reduces': '3' }
    contracts.run()
