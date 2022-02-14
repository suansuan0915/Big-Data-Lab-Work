import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import math

def get_pair(df):
    lst = []
    # collect() returns a list
    for i in df.collect():
        lst.append((tuple(i)[2], tuple(i)[1]))
    return lst

def f_x_y_sum(lst, index):
    res = 0
    for i in lst:
        res += i[index]
    return res

def f_sqr(lst, index):
    res = 0
    for i in lst:
        res += i[index] ** 2
    return res

def f_xy_product(lst):
    res = 0
    for i in lst:
        res += (i[0] * i[1])
    return res


def main(input_dir, keyspace, table_name):
    # main logic starts here
    xy_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table=table_name, keyspace=keyspace).load()
    xy_df.show(10)
    df_count = xy_df.groupBy('host').count()  # .alias('count_requests')
    df_count.show(5)
    df_sum = xy_df.groupBy('host').sum('bytes')  # .alias('sum_request_bytes')
    df_sum.show(5)
    df_c_s = df_sum.join(df_count, ['host'])
    df_c_s.show(5)
    pairs = get_pair(df_c_s)

    n = len(pairs)  # 232
    x_sum = f_x_y_sum(pairs, 0)  # 1972
    y_sum = f_x_y_sum(pairs, 1)  # 36133736
    x_sqr = f_sqr(pairs, 0)  # 32560
    y_sqr = f_sqr(pairs, 1)  # 25731257461526
    xy_product_sum = f_xy_product(pairs)  # 662179733
    print('x_sum', x_sum)
    print('y_sum', y_sum)
    print('x_sqr', x_sqr)
    print('y_sqr', y_sqr)
    print('xy_product_sum', xy_product_sum)

    up = (n * xy_product_sum) - x_sum * y_sum 
    down = math.sqrt(n * x_sqr - (x_sum ** 2) ) * math.sqrt(n * y_sqr - (y_sum ** 2) )
    r = up/down
    r_square = r ** 2
    print("r =", "%.6f" % r)
    print("r^2 =", "%.6f" % r_square)


if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    spark = SparkSession.builder.appName('Spark Cassandra calculation') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_dir, keyspace, table_name)