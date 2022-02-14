import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    # build a dataframe
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
    # weather.show()
    weather = weather.filter((weather['qflag'].isNull()) & (weather['station'].startswith('CA')) & (weather['observation'] == 'TMAX') )
    # keep only 'qflag' == null data
    # weather = weather.filter(weather['station'].startswith('CA'))  # keep only 'station' starts with 'CA'
    # weather = weather.filter(weather['observation'] == 'TMAX')  # Keep maximum temperature observations data
    weather.show()
    # here is Pyspark dataframe, but NOT Pandas dataframe!
    weather_filtered = weather.select('station', 'date', (weather['value']/10).alias('tmax'))
    weather_filtered.show()

    weather_filtered.write.json(output, compression='gzip', mode='overwrite')
    # weather_filtered.write.json(output, mode='overwrite')
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)