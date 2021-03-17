package com.github.fescalhao.spark.example4

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

object HelloSparkSQL extends Serializable {
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      logger.info("Usage: HelloSparkSQL filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    val sampleDF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0))

    sampleDF.createOrReplaceTempView("sample_table")

    val countDF = spark.sql("select Country, count(1) as count from sample_table where Age<40 group by Country")

    logger.info(countDF.collect().mkString(","))

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
