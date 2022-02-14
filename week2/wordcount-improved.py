from pyspark import SparkConf, SparkContext
import sys
import re, string


inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

# get rid of punctuation
# string.punctuation: return all set of punctuation. output: !"#$%&'()*+, -./:;<=>?@[\]^_`{|}~
# re.escape(): Return string with all non-alphanumerics backslashed.
# re.compile(): combine a regular expression pattern into pattern objects, then can be used 
#to search a pattern like that again without rewriting it. 
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))  # this is a pattern of punctuation

def words_once(line):
    for w in wordsep.split(line):
        w = w.lower()
        yield (w, 1)  ## ! aftr this for loop function, we get result like [('word1', 1), ('word2', 1), ...] 
                      #for EACH line input. -> so we need to flatten it to only tuples!
                      # map() vs. flatMap(): the former results in just one item (same number of elements as input),
                      # the latter gets the result split into ['word1', 1, 'word2', 1, ...] pairs (more elements).                    
                      # map() is one-to-one operation, flatMap() is one-to-many. 

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
words_no_empty= words.filter(lambda x: x[0] != '')  # filter out word pairs that are empty
wordcount = words_no_empty.reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)