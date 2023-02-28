from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re
import json

class scam1(MRJob):
    SORT_VALUES = True

    def mapper(self, _, line):
        try:
            if(len(line.split(","))==4):
                fields=line.split(",")
                id=fields[0]
                category=fields[1]
                status=fields[2]
                address=fields[3:]
                yield (address,(2, status, category))

            elif(len(line.split())==3):
                fields=line.split()
                join_address = fields[0]
                join_value = float(fields[1][1:-2])
                join_month = fields[2][1:-1]
                yield (join_address.replace('\"', ''),(1, join_value, join_month.replace('\"', '')))
        except:
            pass

    def reducer(self, key, val):
        values = list(val)
        scamVal ={}
        month = ""
        transaction = 0
        for value in values:
            if value[0]==1:
                month = value[2]
                transaction = value[1]
            elif value[0]==2:
                if value[1] not in scamVal:
                    scamVal[value[1]]=value[2]

        for dictKey in scamVal:
            yield((key,month), (transaction, dictKey, scamVal[dictKey]))

if __name__ == '__main__':
    scam1.JOBCONF= { 'mapreduce.job.reduces': '3' }
    scam1.run()
