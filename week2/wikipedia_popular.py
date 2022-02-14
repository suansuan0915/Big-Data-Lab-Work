from pyspark import SparkConf, SparkContext
import sys
import re, string


inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def split_func(line):
    lst = line.split()
    lst[3]= int(lst[3])
    # lst[2]= list(lst[2])
    return tuple(lst)

def filter_condition(i):
    return i[1] == 'en' and i[2] != 'Main_Page' and (not i[2].startswith("Special:"))

def get_key(pair):
    return pair[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])  # \t: a tab.

## Set two tied titles
# def max_titles(val_1, val_2):  # vals under one key (here it's a pair of (view, title))
#     if val_1[0] > val_2[0]:
#         return val_1
#     elif val_1[0] < val_2[0]:
#         return val_2
#     elif val_1[0] == val_2[0]:
#         val_1[1].append(val_2[1])
#         return val_1


text = sc.textFile(inputs)  # read file in as many lines (e.g. 1000 lines here)
text = text.map(split_func)  # concise version of two lines below 
# text = text.map(lambda x: x.split(' '))  # map() convert one-to-one (still #lines as before)
# text = text.map(lambda y: y[3] = int(y[3]))  # convert view into integer
text = text.filter(filter_condition)  
word_pairs = text.map(lambda w: (w[0], (w[3], w[2])))
max_word_pairs = word_pairs.reduceByKey(max) 
# max_word_pairs = word_pairs.reduceByKey(max_titles)  # get max view count pages 
sorted_max_word_pairs = max_word_pairs.sortBy(get_key)  # sort by key
sorted_max_word_pairs.map(tab_separated).saveAsTextFile(output)
