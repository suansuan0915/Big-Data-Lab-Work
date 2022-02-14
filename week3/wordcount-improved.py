'''
wordcount-improved.py for Assignment_3, different from the one in assignment_2

'''

from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))  # this is a pattern of punctuation

# add more functions as necessary
def words_once(line):
    for w in wordsep.split(line):
        w = w.lower()
        yield (w, 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    # repartition to be faster
    text = text.repartition(20)
    words = text.flatMap(words_once)
    words_no_empty= words.filter(lambda x: x[0] != '')  # filter out word pairs that are empty
    wordcount = words_no_empty.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount-improved code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


# Wordcount-improved repartiton (On cluster):
# No partition: 5.7m
# 20 partitions: 1.7m
# 50 par: 1.9m
# 70 par: 2m.    -> -ls output 70 partitions
# 90 par: 2m
# 100 par: 
# 300 par: 5m
