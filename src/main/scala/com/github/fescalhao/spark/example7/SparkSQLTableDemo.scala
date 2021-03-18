package com.github.fescalhao.spark.example7

import com.github.fescalhao.SparkConfigUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQLTableDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val fileName = "flight-time.parquet"
    logger.info("Creating Spark Session...")
    val spark = SparkSession.builder()
      .config(SparkConfigUtils.getSparkConf("Spark SQL Table Demo"))
      .enableHiveSupport()
      .getOrCreate()

    logger.info(s"Loading $fileName file as a Dataframe")
    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", s"data/$fileName")
      .load()

    logger.info("Creating and Setting the current database for the session")
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    logger.info("Saving the Dataframe as a Spark SQL Table in the AIRLINE_DB database")
    flightTimeParquetDF.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .bucketBy(5, "ORIGIN", "OP_CARRIER")
      .sortBy("ORIGIN", "OP_CARRIER")
      .saveAsTable("flight_times_table")

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}
