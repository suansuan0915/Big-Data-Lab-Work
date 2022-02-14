from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json


# add more functions as necessary
def load(line):
	return json.loads(line)  # cannot use yield here! "yield" can only generate one unbroken object.

def get_pair(line):
	subreddit = line['subreddit']
	score = line['score']
	return (subreddit, (1, score))

def add_pair(pair1, pair2):
	added_pair_0 = pair1[0] + pair2[0]
	added_pair_1 = pair1[1] + pair2[1]
	return (added_pair_0, added_pair_1)

def avg(val_pair):
	avg_score = val_pair[1][1] / val_pair[1][0]
	if avg_score > 0:
		return (val_pair[0], avg_score)

def get_rltscore_pair(item):
	score = item[1][0]['score']
	avg_score = item[1][1]
	relative_score = score / avg_score
	author = item[1][0]['author']
	return (relative_score, author)

def main(inputs, output):
    # main logic starts here
    reddit = sc.textFile(inputs)
    text = reddit.map(load).cache()  # after load, which is expensive
    reddit_by_sub = text.map(lambda c: (c['subreddit'], c))   # get (subreddit, text) pair
    reddits = text.map(get_pair)
    reddit_by_key = reddits.reduceByKey(add_pair)
    avg_score_by_key = reddit_by_key.map(avg)  # get (subreddit, avg_score) pair
    										   # map() deals with  whole (key, val) pair;
    										   # reduceByKey() deals only with value.
    key_comment_avg_pair = reddit_by_sub.join(avg_score_by_key)   # get (subrreddit, (text, avg_score)) pair (by key)
    # get (comment['score']/average, author) pair
    rltscore_author_pair = key_comment_avg_pair.map(get_rltscore_pair)
    sorted_result = rltscore_author_pair.sortBy(lambda x: x[0], ascending=False)
    # sorted_result_json = json.dumps(sorted_result)

    sorted_result.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)