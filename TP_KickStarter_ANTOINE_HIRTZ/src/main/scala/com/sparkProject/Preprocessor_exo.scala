package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{regexp_replace, from_unixtime, to_date}

object Preprocessor_exo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
        /*** Partitionnement en 4 machines */
      "spark.default.parallelism" -> "4",
      "spark.sql.shuffle.partitions" -> "4"
    ))

    /** Déclaration de l'objet SparkSession: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession */

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()

    import spark.implicits._

    /*******************************************************************************
      *
      *       TP 2-3
      *
      *       - Charger un fichier csv dans un dataFrame
      *       - Pre-processing: cleaning, filters, feature engineering => filter, select, drop, na.fill, join, udf, distinct, count, describe, collect
      *       - Sauver le dataframe au format parquet
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/

    /** 1 - CHARGEMENT DES DONNEES **/

    /** 1.a - import du fichier .csv en format DataFrame: */

    val train_df_schema = StructType(
      StructField("project_id", StringType, false) ::
      StructField("name", StringType, true) ::
      StructField("desc", StringType, true) ::
      StructField("goal", IntegerType, true) ::
      StructField("keywords", StringType, true) ::
      StructField("disable_communication", BooleanType, true) ::
      StructField("country", StringType, true) ::
      StructField("currency", StringType, true) ::
      StructField("deadline", StringType, true) ::
      StructField("state_changed_at", StringType, true) ::
      StructField("created_at", StringType, true) ::
      StructField("launched_at", StringType, true) ::
      StructField("backers_count", IntegerType, true) ::
      StructField("final_status", IntegerType, false) :: Nil)

    val train_df_raw: DataFrame = spark
      .read
      .format("csv")
      .option("header", "true")
      /* specifies a string that indicates a null value, any fields matching this string will be set as nulls in the DataFrame.
       * in the present case, set "false" string as "null" value. */
      .option("nullValue", "false")
      // specifies to treat empty values as nulls during the csv import. */
      .option("treatEmptyValuesAsNulls", "true")
      // drops lines which have fewer or more tokens than expected or tokens which do not match the schema.
      //  .option("mode", "DROPMALFORMED") //
      .schema(train_df_schema)
      .load("/Users/antoinehirtz/Documents/Ms-BigData/P1/INF729-HADOOPframework/TP-SPARK/TP_ParisTech_2017_2018_starter/train.csv")

    /** 1.b - affichage du nombre de lignes et de colonnes: */

    val n_rows: Long = train_df_raw.count()
    val n_columns: Int = train_df_raw.head().size

    println(" ### Number of rows: " + n_rows)
    println(" ### Number of columns: " + n_columns)

      /*
         Number of rows:                111792
           - with option DROPMALFORMED: 102482
           - 9310 malformed rows deleted !
         Number of columns:             14
      */

    /** 1.c - affichage du contenu du DataFrame: */

    train_df_raw.show()

    /** 1.d - affichage du schéma du DataFrame: */

    train_df_raw.printSchema()

    /** 1.e - assigner le type Integer aux colonnes contenant des entiers: */

    /* se référer à la déclaration du schema de dataframe. */

    /** convertir les colonnes de type UNIX-timestamp en format Date: **/

    val train_df_ts = train_df_raw.withColumn("deadline", from_unixtime($"deadline"))
                                  .withColumn("state_changed_at", from_unixtime($"state_changed_at"))
                                  .withColumn("created_at", from_unixtime($"created_at"))
                                  .withColumn("launched_at", from_unixtime($"launched_at"))

    /** 2 - CLEANING **/

    /** 2.a -	Pour une classification, l’équilibrage entre les différentes classes cibles dans les données d’entraînement doit être contrôlé (et éventuellement corrigé).
      * a.	Afficher une description statistique des colonnes de type Int (avec .describe().show ): **/

    train_df_ts.select("goal", "backers_count", "final_status").describe().show()

    /** Filtrer les valeurs aberrantes dans la colonne 'Final_status': **/

    val train_df_clean = train_df_ts.filter($"final_status" isin(0, 1))

    train_df_clean.select("goal", "backers_count", "final_status").describe().show()

    /** 2.b -  Observer les autres colonnes, et proposer des cleanings à faire sur les données: faites des groupBy count, des show, des dropDuplicates. **/

    // "project_id", "name",  "desc",  "keywords",  "disable_communication", "country", "currency" //



    train_df_ts.select("project_id").dropDuplicates()

    /** 2.c filtre sur le DataFrame: */

    /*train_df.filter($"goal" >= 0)*/

    /** 2.d résumé du DataFrame: */

    /*train_df.describe().show()*/

    /** FEATURE ENGINEERING: Ajouter et manipuler des colonnes **/

  }

}
