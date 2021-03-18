package com.github.fescalhao.spark.example6

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, date_format, spark_partition_id}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object DataSinkDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val fileName = "flight-time.parquet"

    logger.info("Creating Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Data Sink Demo"))
      .getOrCreate()

    logger.info(s"Loading $fileName file")
    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", s"data/$fileName")
      .load()

    logger.info(s"Num Partitions Before: ${flightTimeParquetDF.rdd.getNumPartitions}")
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    val partitionedDF = flightTimeParquetDF.repartition(2)

    logger.info(s"Num Partitions After: ${partitionedDF.rdd.getNumPartitions}")
    partitionedDF.groupBy(spark_partition_id()).count().show()

    val canceledFlightsByDistance = countCancelledFlightsByDistance(partitionedDF)
    canceledFlightsByDistance.show(10)

    logger.info("Saving cancelled flights DataFrame")
    canceledFlightsByDistance.write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/avro/")
      .save()

    logger.info("Saving flights DataFrame partitioned by OP_CARRIER and ORIGIN")
    partitionedDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/json/")
      .option("maxRecordsPerFile", 10000)
      .partitionBy("OP_CARRIER", "ORIGIN")
      .save()

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }

  def countCancelledFlightsByDistance(flightDF: DataFrame): DataFrame = {
    flightDF.where("CANCELLED > 0" )
      .select(
        col("ORIGIN_CITY_NAME").as("origin_city"),
        col("DISTANCE").as("distance")
//      date_format(col("FL_DATE"), "yyyy").as("flight_date"), useful example to format dates
      )
      .groupBy("origin_city", "distance")
      .count()
      .withColumnRenamed("count", "cancelled_flights")
    }
}
