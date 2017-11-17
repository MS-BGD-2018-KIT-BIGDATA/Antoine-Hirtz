package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Trainer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.driver.maxResultSize" -> "2g"
    ))

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()

    /*******************************************************************************
      *
      *       TP 4-5
      *
      *       - lire le fichier sauvegardé précédemment
      *       - construire les Stages du pipeline, puis les assembler
      *       - trouver les meilleurs hyperparamètres pour l'entraînement du pipeline avec une grid-search
      *       - Sauvegarder le pipeline entraîné
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/

   /** LOADING PREPROCESSED DATASET **/

    val df = spark.sqlContext.read.load("/TP_ParisTech_2017_2018_starter/prepared_trainingset/*")
~
    /** TF-IDF **/

    /** a. **/

    val tokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")
      .setPattern("\\W+")
      .setGaps(true)

    /** b. **/

    val remover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered")

    /** c. **/

    val tf = new CountVectorizer()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("vectorized")

    /** d. **/

    val idf = new IDF()
      .setInputCol(tf.getOutputCol)
      .setOutputCol("tfidf")

    /** e. **/

    val indexer_country = new StringIndexer()
      .setInputCol("country2")
      .setOutputCol("country_indexed")
      .fit(df)

    /** f. **/

    val indexer_currency = new StringIndexer()
      .setInputCol("currency2")
      .setOutputCol("currency_indexed")
      .fit(df)

    /** VECTOR ASSEMBLER **/

    /** g. **/

    val assembler = new VectorAssembler()
      .setInputCols(Array("tfidf", "days_campaign", "hours_prepa", "goal", "country_indexed", "currency_indexed"))
      .setOutputCol("features")

    /** MODEL **/

    /** h. **/

    val lr = new LogisticRegression()
      .setElasticNetParam(0.0)
      .setFitIntercept(true)
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setStandardization(true)
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")
      .setThresholds(Array(0.7, 0.3))
      .setTol(1.0e-6)
      .setMaxIter(300)

    /** PIPELINE **/

    /** i. **/

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, tf, idf, indexer_country, indexer_currency, assembler, lr))

    /** TRAINING AND GRID-SEARCH **/

    /** j. **/

    val Array(training, test) = df.randomSplit(Array(0.90, 0.10))

    /** k. **/

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for tf.minDF and 4 values for lr.regParam,
    // this grid will have 3 x 4 = 12 parameter settings for TrainValidationSplit to choose from.

    val paramGrid = new ParamGridBuilder()
      .addGrid(tf.minDF, Array(55.0, 75.0, 95.0))
      .addGrid(lr.regParam, Array(10E-8, 10E-6, 10E-4, 10E-2))
      .build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("final_status")
      .setPredictionCol("predictions")
      .setMetricName("f1")

    // We use the trainValidationSplit() method for a model validation with hyper-parameter tuning.
    // trainValidationSplit() is similar to CrossValidator(), except that it only splits the set once.

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      // 70% of the data will be used for training and the remaining 30% for validation.
      .setTrainRatio(0.70)

    /** FIT AND SCORE **/

    val model = trainValidationSplit.fit(training)
    val df_withPredictions = model.transform(test)

    /** l. **/

    val Test_f1Score = evaluator.evaluate(df_withPredictions)
    println("F1 score on test data: " + Test_f1Score)

    /** m. **/

    df_withPredictions.groupBy("final_status", "predictions").count.show()

  }

}
