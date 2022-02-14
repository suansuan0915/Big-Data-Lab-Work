from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def select_fields(line):
    line = json.loads(line)  # parse each JSON string to a Python object.
    subreddit = line['subreddit']
    score = int(line['score'])  # need to convert score to integer!
    author = line['author']
    return (subreddit, score, author)

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    reddit_fields = text.map(select_fields)
    reddit_with_e = reddit_fields.filter(lambda x: 'e' in x[0])
    
    # reddit_cached = reddit_with_e.cache()  # cache(): mark an RDD to be persisted. 
    # The first time RDD is computed in an action, it will be kept in memory on the nodes.

    reddit_score_positive = reddit_with_e.filter(lambda x: x[1] > 0)
    reddit_score_negative = reddit_with_e.filter(lambda x: x[1] < 0)
    reddit_score_positive.map(json.dumps).saveAsTextFile(output + '/positive')
    reddit_score_negative.map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit ETL')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)