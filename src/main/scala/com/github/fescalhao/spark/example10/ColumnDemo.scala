package com.github.fescalhao.spark.example10

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import functions._

object ColumnDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Column Demo"))
      .getOrCreate()

    logger.info("Loading the file")
    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight-*.parquet")
      .load()

    logger.info("Selecting 5 rows using expr to define a new column")
    flightTimeParquetDF.select(
      column("OP_CARRIER"),
      col("ORIGIN"),
      col("DISTANCE"),
      col("DEST"),
      expr("ORIGIN || ' -> ' || DEST").as("origin_dest")
    ).show(5)

    logger.info("Select 5 rows using column object expressions to define a new column")
    flightTimeParquetDF.select(
      column("OP_CARRIER"),
      col("ORIGIN"),
      col("DISTANCE"),
      col("DEST"),
      concat(col("ORIGIN"), expr("' >>> '"), col("DEST"))
    ).show(5)

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}
