package com.github.fescalhao.spark.example1

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

object HelloSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }

    logger.info("Starting Hello Spark")
    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    // Process data
    val sampleDF = loadSampleDF(spark, args(0))
    val partitionedSampleDF = sampleDF.repartition(2)

    val countDF = countByCountry(partitionedSampleDF)

    logger.info(countDF.collect().mkString("->"))

    logger.info("Finished Hello Spark")
//    Used to check the Spark UI at localhost:4040
//    scala.io.StdIn.readLine()
    spark.stop()
  }

  def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((x, y) =>
      sparkConf.set(x.toString, y.toString)
    )

    sparkConf
  }

  def loadSampleDF(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(dataFile)
  }

  def countByCountry(sampleDF: DataFrame): DataFrame = {
    sampleDF.where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }
}
