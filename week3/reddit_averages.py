from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json


# add more functions as necessary
def load(line):
	return json.loads(line)  # cannot use yield here! "yield" can only generate one unbraken object.

def get_pair(line):
	subreddit = line['subreddit']
	score = line['score']
	return (subreddit, (1, score))

def add_pair(pair1, pair2):
	added_pair_0 = pair1[0] + pair2[0]
	added_pair_1 = pair1[1] + pair2[1]
	return (added_pair_0, added_pair_1)

def avg(val_pair):
	avg_score = val_pair[1][1] / val_pair[1][0]  # coding style: unpacking tuple
	return json.dumps((val_pair[0], avg_score))

def output_format(x):
	result = json.dumps(x)
	return result

def main(inputs, output):
    # main logic starts here
    reddit = sc.textFile(inputs)
    text = reddit.map(load)   
    reddits = text.map(get_pair)
    reddit_by_key = reddits.reduceByKey(add_pair)
    avg_score_by_key = reddit_by_key.map(avg)  # map() deals with  whole (key, val) pair;
    										   # reduceByKey() deals only with value.
    avg_score_by_key.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)