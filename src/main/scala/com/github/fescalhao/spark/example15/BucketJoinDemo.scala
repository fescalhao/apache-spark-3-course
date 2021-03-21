package com.github.fescalhao.spark.example15

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object BucketJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Bucket Join Demo"))
      .getOrCreate()

    logger.info("Loading flight time file 1")
    val flightTimeDF1 = spark.read
      .format("json")
      .option("path", "data/d1/")
      .load()

    logger.info("Loading flight time file 2")
    val flightTimeDF2 = spark.read
      .format("json")
      .option("path", "data/d2/")
      .load()

    logger.info("Creating FLIGHT_DB database")
    spark.sql("CREATE DATABASE IF NOT EXISTS FLIGHT_DB")
    spark.sql("USE FLIGHT_DB")

    logger.info("Saving flight time data 1 to FLIGHT_DB")
    flightTimeDF1.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .bucketBy(2, "id")
      .saveAsTable("FLIGHT_DB.flight_data1")

    logger.info("Saving flight time data 2 to FLIGHT_DB")
    flightTimeDF2.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .bucketBy(2, "id")
      .saveAsTable("FLIGHT_DB.flight_data2")

    logger.info("Loading flight_data1 data")
    val flightTable1 = spark.read.table("FLIGHT_DB.flight_data1")

    logger.info("Loading flight_data2 data")
    val flightTable2 = spark.read.table("FLIGHT_DB.flight_data2")

    logger.info("Setting autoBroadcastJoinThreshold to -1 to prevent broadcasting")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    logger.info("Defining the join expression")
    val flightJoinExpr = flightTable1.col("id") === flightTable2.col("id")

    logger.info("Joining both tables")
    val joinDF = flightTable1.join(flightTable2, flightJoinExpr, "inner")
    joinDF.show()

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}
