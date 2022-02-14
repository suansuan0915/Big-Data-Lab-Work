import sys

from pyspark.ml.classification import GBTClassifier
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor
from pyspark.ml.evaluation import Evaluator, RegressionEvaluator
spark = SparkSession.builder.appName('weather train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(inputs, output):
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    query = ''' SELECT today.day_of_yr AS day_of_yr,
                       today.station AS station,
                       today.latitude AS latitude,
                       today.longitude AS longitude,
                       today.elevation AS elevation,
                       yesterday.tmax AS yesterday_tmax,
                       today.tmax AS tmax 
              FROM __THIS__ as today
              INNER JOIN __THIS__ as yesterday
              ON date_sub(today.date, 1) = yesterday.date
              AND today.station = yesterday.station '''
    sqltrans = SQLTransformer(statement="SELECT DAYOFYEAR(date) AS day_of_yr, * FROM __THIS__")
    ytmax_sqltrans = SQLTransformer(statement=query)
    
    ## score w "yesterday_tmax"
    vecAssr = VectorAssembler(inputCols=['day_of_yr', 'latitude', 'longitude', 'elevation', 'yesterday_tmax'], outputCol='features')
    clsr = GBTRegressor(featuresCol='features', labelCol='tmax', maxDepth=5, maxIter=100)
    pipline = Pipeline(stages=[sqltrans, ytmax_sqltrans, vecAssr, clsr])
    w_model = pipline.fit(train)
    v_df = w_model.transform(validation)
    v_df.show(10)
    evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')

    # on validation set
    # r2_err = r2_evaluator.evaluate(v_df)
    # print('r2 of this GBT model on validation data (w "yesterday_tmax"):', r2_err)
    # err = evaluator.evaluate(v_df)
    # print('root mean square error (RMSE) of this GBT model on validation data (w "yesterday_tmax"):', err)


    ## score w/o "yesterday_tmax"
    vecAssr_no_ytmax = VectorAssembler(inputCols=['day_of_yr', 'latitude', 'longitude', 'elevation'], outputCol='features')
    pipline_no_ytmax = Pipeline(stages=[sqltrans, vecAssr_no_ytmax, clsr])
    w_model_no_ytmax = pipline_no_ytmax.fit(train)

    # on training set
    v_df_n_train = w_model_no_ytmax.transform(train)
    r2_err_t = r2_evaluator.evaluate(v_df_n_train)
    print('r2 of this GBT model on training data (w/o "yesterday_tmax"):', r2_err_t)
    err_t = evaluator.evaluate(v_df_n_train)
    print('root mean square error (RMSE) of this GBT model on training data (w/o "yesterday_tmax"):', err_t)
 
    # on validation set
    v_df_no_ytmax = w_model_no_ytmax.transform(validation)
    v_df_no_ytmax.show(10)
    r2_err_no_ytmax = r2_evaluator.evaluate(v_df_no_ytmax)
    print('r2 of this GBT model on validation data (w/o "yesterday_tmax"):', r2_err_no_ytmax)
    err_no_ytmax = evaluator.evaluate(v_df_no_ytmax)
    print('root mean square error (RMSE) of this GBT model on validation data (w/o "yesterday_tmax"):', err_no_ytmax)

    # save the trained model to a file
    w_model.write().overwrite().save(output)
    # w_model_no_ytmax.write().overwrite().save(output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    # main(inputs)
    output = sys.argv[2]
    main(inputs, output)