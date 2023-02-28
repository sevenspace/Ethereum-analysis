import pyspark
import re

sc = pyspark.SparkContext()

#Reads transactions table
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False
        float(fields[3])
        return True

    except:
        return False

lines = sc.textFile("/data/ethereum/transactions")
clean_lines = lines.filter(is_good_line)
features=clean_lines.map(lambda l: (l.split(',')[2],float(l.split(',')[3]) ))
sum_trans= features.reduceByKey(lambda a,b: a+b)
sum_trans.persist()


#Read contracts table
def is_good_line2(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        return False

lines2 = sc.textFile("/data/ethereum/contracts")
clean_lines2 = lines2.filter(is_good_line2)
#yields like mapper and formats data
features2=clean_lines2.map(lambda l: (l.split(',')[0], 0 ))
joinResults = features2.join(sum_trans)
top10 = joinResults.takeOrdered(10, lambda x: -x[1][1])
for record in top10:
    print("{},{}".format(record[0],record[1][1]))
