from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

WORD_REGEX = re.compile(r"\b\w+\b")

class numTrans(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==7):
                    time_epoch = int(fields[6])
                    month = time.strftime("%Y-%m",time.gmtime(time_epoch)) #returns month of the year
                    value = fields[3]
                    yield (month, float(value))
        except:
            pass
            #no need to do anything, just ignore the line, as it was malformed

    def reducer(self, month, value):
        valueSum = 0
        counter = 0

        for values in value:
            valueSum = valueSum + values
            counter = counter + 1
        average = valueSum/counter
        yield(month, average)

if __name__ == '__main__':
    numTrans.JOBCONF= { 'mapreduce.job.reduces': '3' }
    numTrans.run()
