from mrjob.job import MRJob
import re
import time

class gasAverage(MRJob):
    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==7):
                time_epoch = int(fields[6])
                month = time.strftime("%Y-%m", time.gmtime(time_epoch))
                gasPrice = int(fields[5])

                yield(month, (gasPrice, 1))
        except:
            pass

    def combiner(self, month, val):
        count = 0
        total = 0

        for value in val:
            total = total + value[0]
            count = count + value[1]
        yield(month, (total, count))

    def reducer(self, month, val):
        count = 0
        total = 0
        average = 0

        for value in val:
            total = total + value[0]
            count = count + value[1]
        average = total/count

        yield(month, average)

if __name__ == '__main__':
    gasAverage.JOBCONF= { 'mapreduce.job.reduces': '3' }
    gasAverage.run()
