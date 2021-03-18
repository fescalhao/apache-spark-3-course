package com.github.fescalhao.spark.example4

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object HelloSparkSQL extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      logger.info("Usage: HelloSparkSQL filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .config(getSparkConf("Hello Spark SQL"))
      .getOrCreate()

    val sampleDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    sampleDF.createOrReplaceTempView("sample_table")

    val countDF = spark.sql("select Country, count(1) as count from sample_table where Age<40 group by Country")

    logger.info(countDF.collect().mkString(","))

    spark.stop()
  }
}
