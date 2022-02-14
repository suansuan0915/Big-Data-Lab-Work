import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import skimage

from colour_tools import colour_schema, rgb2lab_query, plot_predictions

def main(inputs):
    # load data
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    # create vector of length 3
    vecAmbr = VectorAssembler(inputCols=['R', 'G', 'B'], outputCol='features', handleInvalid="keep")
    # transform string label to numeric
    strIdr = StringIndexer(inputCol='word', outputCol='label')  # stringOrderType='frequencyDesc'
    clsfr = MultilayerPerceptronClassifier(layers=[3, 10, 11], maxIter=900)  # 3 features, 11 outputs, #layer between
    # build pipline to predict RGB colours -> word
    pipline_tr = Pipeline(stages = [vecAmbr, strIdr, clsfr])
    rgb_model = pipline_tr.fit(train)
    # why no transform to validation data here?
    # evaluate the goodness on validation data
    v_df = rgb_model.transform(validation)
    v_df.select('features','label','rawPrediction','probability','prediction').show(10)
    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName = 'accuracy')
    score = evaluator.evaluate(v_df)
    print('Validation score for RGB model:', score)

    # plot
    plot_predictions(rgb_model, 'RGB', labelCol='word')

    # convert RGB to LAB
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sqltrans = SQLTransformer(statement=rgb_to_lab_query)
    vecAmbr_lab = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol='features', handleInvalid="keep")
    strIdr_lab = StringIndexer(inputCol='word', outputCol='label')
    clsfr_lab = MultilayerPerceptronClassifier(layers=[3, 10, 11], maxIter=900)
    pipline_lab = Pipeline(stages=[sqltrans, vecAmbr_lab, strIdr_lab, clsfr_lab])
    lab_model = pipline_lab.fit(train)
    v_lab = lab_model.transform(validation)
    v_lab.show(10)
    score_lab = evaluator.evaluate(v_lab)
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model:', score_lab)


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)