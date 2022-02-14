import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, DataFrameReader

# add more functions as necessary
@functions.udf(returnType=types.StringType())
def path_to_hour(filename):
    name = filename.split('/')[-1]
    start = len('pagecounts-')
    length = len('YYYYMMDD-HH')
    dayhour = name[start:(start + length)]  
    return dayhour

def main(inputs, output):
    # main logic starts here
    wiki_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('view', types.LongType()),
        types.StructField('size_content', types.LongType())
        ])
    # first, add "filename" as a new column
    wikipages = spark.read.csv(inputs, sep = ' ', schema = wiki_schema).withColumn('filename', functions.input_file_name())
    # withColumn(): Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
    # then, extract "day_hour" column from "filename" column. If no @ above, we can add the following udf line.
    # get_dayhour = functions.udf(path_to_hour, returnType=types.StringType())
    wiki_dayhour = wikipages.withColumn('hour', path_to_hour(wikipages['filename']))
    # or: wiki_dayhour = wikipages.select(wikipages['language'], wikipages['title'], wikipages['view'], path_to_hour(wikipages['filename']).alias('hour'))
    wiki_dayhour = wiki_dayhour.filter((wiki_dayhour['language'] == 'en') & (wiki_dayhour['title'] != 'Main_Page') & (wiki_dayhour['title'].startswith('Special:')==False))
    wiki_dayhour = wiki_dayhour.cache()
    # find the largest number of page views in each hour.
    maxage = wiki_dayhour.groupby(wiki_dayhour['hour']).max('view').withColumnRenamed('max(view)', 'view')
    res = maxage.join(wiki_dayhour,['hour', 'view'], 'inner')
    # res = wiki_dayhour.join(functions.broadcast(maxage),['hour', 'view'], 'inner')   # broadcast join
    res = res.select(res['hour'], res['title'], res['view'])
    res = res.orderBy(res['hour'])
    res.show(10)
    res.explain()

    res.write.json(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
