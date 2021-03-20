package com.github.fescalhao.spark.example11

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, udf}

object UDFDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("UDF Demo"))
      .getOrCreate()

    logger.info("Loading the data file...")
    val surveyDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("path", "data/survey.csv")
      .load()

    logger.info("Registering function as a Dataframe UDF")
    val parseGenderUDF = udf(parseGender(_: String): String)

    logger.info("Transforming Dataframe using UDF")
    val surveyTransformedColDF = surveyDF.withColumn("Gender", parseGenderUDF(col("Gender")))
    surveyTransformedColDF.show(10, truncate = false)

    logger.info("Registering function as a SQL Function")
    spark.udf.register("parseGenderUDF", parseGender(_: String): String)

    logger.info("Transforming Dataframe using SQL Function")
    val surveyTransformedSQLDF = surveyDF.withColumn("Gender", expr("parseGenderUDF(Gender)"))
    surveyTransformedSQLDF.show(10, truncate = false)

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }

  def parseGender(s: String): String = {
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if (femalePattern.findFirstIn(s.toLowerCase).nonEmpty)
      "Female"
    else if (malePattern.findFirstIn(s.toLowerCase).nonEmpty)
      "Male"
    else
      "Unknown"
  }
}
