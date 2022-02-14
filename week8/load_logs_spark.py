from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, functions
from pyspark.sql.types import StructType,StructField, IntegerType, StringType, TimestampType
import math
import sys
import os
import gzip
import re
from uuid import uuid4
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import *


line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def get_xy(line):
    match_or_not = line_re.match(line)
    if match_or_not :
        return match_or_not.groups() 
    else:
        return None

def lines_to_rows(line):
    l = len(line)
    host = line[0]
    id_ = str(uuid4())
    date_time = datetime.strptime(line[1], '%d/%b/%Y:%H:%M:%S')
    path_r = str(line[2])
    byte = int(line[l-1])
    return Row(host, id_, byte, date_time, path_r)



def main(input_dir, keyspace, table_name):
    text = sc.textFile(input_dir)
    text.repartition(44)
    xy = text.map(get_xy)  # print rdd: xy.take(5) 
    xy = xy.filter(lambda a: a != None)
    xy_rdd = xy.map(lines_to_rows)
    schema = StructType([
        StructField('host', StringType()),
        StructField('id', StringType()),
        StructField('bytes', IntegerType()),
        StructField('date_time', TimestampType()),
        StructField('r_path', StringType())
          ])
    xy_df = spark.createDataFrame(xy_rdd, schema=schema).cache()
    xy_df.show(10)
    # xy_df.repartition(44)

    xy_df.write.format("org.apache.spark.sql.cassandra").mode("overwrite") \
    .option('confirm.truncate', 'true').options(table=table_name, keyspace=keyspace).save()



if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)