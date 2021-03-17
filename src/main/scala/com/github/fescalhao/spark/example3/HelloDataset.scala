package com.github.fescalhao.spark.example3

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.Properties
import scala.io.Source

object HelloDataset extends Serializable {

  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      logger.info("Usage: HelloDataset filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    val rawDF: Dataset[Row] = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0))

    import spark.implicits._
    val dataSetRecord: Dataset[SampleRDDRecord] = rawDF
      .select("Age", "Gender", "Country", "state")
      .as[SampleRDDRecord]

    // Type safe filter
    val filteredDS = dataSetRecord.filter(row => row.Age < 40)
    val groupedDS = filteredDS.groupByKey(key => key.Country).count()

    // Runtime filter (Would give a runtime error)
    // val filteredDF = dataSetRecord.filter("age < 40")
    // val groupedDF = filteredDF.groupBy("Country").count()

    logger.info(groupedDS.collect().mkString(","))

    spark.stop()
  }

  def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())

    props.forEach((k, v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf
  }
}
