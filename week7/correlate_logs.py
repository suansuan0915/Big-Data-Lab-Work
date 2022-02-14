from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, functions
from pyspark.sql.types import StructType,StructField, IntegerType, StringType
import sys
import re
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

# add more functions as necessary
def get_xy(line):
    match_or_not = line_re.match(line)
    if match_or_not :
        return match_or_not.groups() 
    else:
        return None

def lines_to_rows(line):
    l = len(line)
    host = line[0]
    byte = int(line[l-1])
    return Row(x=host, y=byte)

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


def main(inputs):
    # main logic starts 
    spark = SparkSession(sc)
    text = sc.textFile(inputs)
    xy = text.map(get_xy)  # print rdd: xy.take(5) 
    xy = xy.filter(lambda a: a != None)
    xy_rdd = xy.map(lines_to_rows)
    schema = StructType([
        StructField('host', StringType()),
        StructField('byte', IntegerType())  ])
    xy_df = spark.createDataFrame(xy_rdd, schema=schema).cache()
    xy_df.show(5)
    df_count = xy_df.groupBy('host').count()  # .alias('count_requests')
    df_count.show(5)
    df_sum = xy_df.groupBy('host').sum('byte')  # .alias('sum_request_bytes')
    df_sum.show(5)
    df_c_s = df_sum.join(df_count, ['host'])
    df_c_s.show(5)
    pairs = get_pair(df_c_s)
    print(pairs[:10])

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
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)