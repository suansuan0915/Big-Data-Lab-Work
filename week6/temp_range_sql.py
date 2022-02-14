import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
    ])
    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView('weather')
    df = spark.sql(""" SELECT * --qflag, observation 
                        FROM weather
                        WHERE (qflag IS NULL) and (observation IN ('TMAX', 'TMIN')) """)
    df.createOrReplaceTempView('df')
    df_min = spark.sql(""" SELECT `date`, station, value FROM df
                            WHERE observation = 'TMIN' 
                            ORDER BY `date` """)
    df_min.createOrReplaceTempView('df_min')
    df_max = spark.sql(""" SELECT `date`, station, value FROM df
                            WHERE observation = 'TMAX'  
                            ORDER BY `date` """)
    df_max.createOrReplaceTempView('df_max')
    df_joined = spark.sql(""" SELECT df_min.`date` AS `date`, df_min.station AS `station`, df_max.value AS max_val, df_min.value AS min_val 
                            FROM df_min JOIN df_max 
                            ON df_min.`date` = df_max.`date` and df_min.station = df_max.station
                            ORDER BY `date` """)
    df_joined.createOrReplaceTempView('df_joined')
    df_r = spark.sql(""" SELECT `date`, station, (max_val - min_val)/10 AS range FROM df_joined
                         ORDER BY `date` """)
    df_r.createOrReplaceTempView('df_r')
    df_r = df_r.cache()
    df_r.show(5)
    res = spark.sql(""" SELECT `date`, max(range) AS range FROM df_r 
                        GROUP BY `date` 
                        ORDER BY `date` """)
    res.show(5)
    res.createOrReplaceTempView('res')
    res = spark.sql(""" SELECT res.`date` AS `date`, station, res.range AS range 
                        FROM res JOIN df_r
                        ON df_r.`date` = res.`date` and res.range = df_r.range
                        ORDER BY `date` , station """)
    
    res.show()

    res.write.csv(output, mode='overwrite')

    # df = weather.filter((weather['qflag'].isNull()) & ((weather['observation'] =='TMAX') | (weather['observation'] == 'TMIN'))) 
    # df_min = df.filter(df['observation'] == 'TMIN')
    # df_min = df_min.select('date', 'station', df_min['observation'].alias('min_ob'), df_min['value'].alias('min_val')).orderBy('date') 
    # df_max = df.filter(df['observation'] == 'TMAX')
    # df_max = df_max.select('date', 'station', df_max['observation'].alias('max_ob'), df_max['value'].alias('max_val')).orderBy('date') 
    # df_joined = df_min.join(df_max, ['date', 'station'], 'inner')
    # df_r = df_joined.withColumn('range', (df_joined['max_val'] - df_joined['min_val'])/10).orderBy('date')
    # df_r = df_r.cache()
    # df_r.show(5)
    # maxage = df_r.groupby('date').max('range').withColumnRenamed('max(range)', 'range')
    # maxage.show(5)
    # df_joined = maxage.join(df_r, ['date', 'range'], 'inner') 
    # res = df_joined.select('date', 'station', 'range')
    # res.show(5)
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)