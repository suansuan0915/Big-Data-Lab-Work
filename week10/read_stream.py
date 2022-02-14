import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions
from kafka import KafkaConsumer


def f_slope(df):
    beta = (df['xy_sum'] - 1/df['n'] * df['x_sum'] * df['y_sum']) / (df['x_square'] - 1/df['n'] * pow(df['x_sum'], 2))
    return beta

def f_intercept(df):
    alpha = df['y_sum'] / df['n'] - df['slope_beta'] * df['x_sum'] / df['n']
    return alpha

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    # check data loaded: 
    # consumer = KafkaConsumer(topic, bootstrap_servers=['node1.local', 'node2.local'])
    # for msg in consumer:
    #     print(msg.value.decode('utf-8'))

    values_xy = values.withColumn('xy', functions.split(values['value'], ' '))
    df = values_xy.select(values_xy['xy'].getItem(0).cast('float').alias('x'), values_xy['xy'].getItem(1).cast('float').alias('y')) 
    
    df_cal = df.select(functions.count('x').alias('n'), functions.sum('x').alias('x_sum'), \
        functions.sum('y').alias('y_sum'), functions.sum(pow(df['x'], 2)).alias('x_square'),\
        functions.sum(df['x'] * df['y']).alias('xy_sum'))

    df_cal.show(10)

    df_cal1 = df_cal.withColumn('slope_beta', f_slope(df_cal))
    df_cal2 = df_cal1.withColumn('intercept_alpha', f_intercept(df_cal1))
    res = df_cal2.select('slope_beta', 'intercept_alpha')

    # control the loop to be finite
    stream = res.writeStream.format('console').outputMode('complete').start()
    stream.awaitTermination(600)

if __name__ == '__main__':
    topic = sys.argv[1]  # topic in Kafka equals to inputs
    spark = SparkSession.builder.appName('read stream').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    main(topic)




