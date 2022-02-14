import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import Evaluator, RegressionEvaluator
from datetime import datetime
from pyspark.ml.pipeline import PipelineModel

spark = SparkSession.builder.appName('weather train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+


t_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(model):
    tmr_data = spark.createDataFrame([('SFU Lab', datetime.strptime('2021-11-19', '%Y-%m-%d'), 49.2771, -122.9146, 330.0, 12.0), \
                                      ('SFU Lab', datetime.strptime('2021-11-20', '%Y-%m-%d'), 49.2771, -122.9146, 330.0, 10.0)], \
                                      schema=t_schema)
    tmr_data.cache()

    t_model = PipelineModel.load(model)
    t_df = t_model.transform(tmr_data)
    t_df.show(3)

    prediction = t_df.select('prediction').collect()[0]['prediction']
    print('Predicted tmax tomorrow:', prediction)

if __name__ == '__main__':
    model = sys.argv[1]
    main(model)
    