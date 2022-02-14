from pyspark import SparkConf, SparkContext
import sys
import random
# import numpy as np
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


# add more functions as necessary
def calculate_iterations(iter):
    sum_iterations = 0
    iterations = 0
    random.seed()   # if in for-loop, called thousands of times -> lots of cost
    for i in range(iter):
        sum = 0.0
        while sum < 1:
            rand_num = random.uniform(0, 1)
            sum += rand_num
            iterations += 1
    sum_iterations += iterations 
    return sum_iterations


def add(x, y):
    return x + y


def main(inputs):
    # main logic starts here
    batches =  20  # (#cores * 4/6/8/10/...), usually 2 or 3 digits
    int_inputs = int(inputs)
    samples = sc.parallelize([int(int_inputs/batches)] * batches, numSlices=batches)
    # OR: samples = sc.parallelize(np.repeat(int(int_inputs/batches), batches), numSlices=batches)
    # samples = sc.range(0, int_inputs, 1, numSlices=batches)  # 100 partitions
    iterations_lst = samples.map(calculate_iterations)
    total_iterations = iterations_lst.reduce(add)
    print(total_iterations/int_inputs)  


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    # output = sys.argv[2]  # no output here in command line
    main(inputs)