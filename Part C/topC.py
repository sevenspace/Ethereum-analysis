from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re

class topc(MRJob):

    def steps(self):
            return [MRStep(mapper=self.mapper_block,
                            reducer=self.reducer_sum),
                    MRStep(mapper=self.mapper_none,
                            reducer=self.reducer_sort)]

    def mapper_block(self, _, line):
        try:
            if(len(line.split(","))==9):
                fields=line.split(",")
                size=fields[4]
                miner=fields[2]
                yield(miner, (int(size)))
        except:
            pass

    def reducer_sum(self, miner, val):
        total = 0
        for values in val:
            total = total + values
        yield(miner, total)

    def mapper_none(self, miner, val):
        yield(None, (miner, val))

    def reducer_sort(self, block, val):
        sorted_values = sorted(val, reverse=True, key=lambda x: x[1])
        for y in range (10):
            yield(block, sorted_values[y])

if __name__ == '__main__':
    topc.JOBCONF= { 'mapreduce.job.reduces': '3' }
    topc.run()
