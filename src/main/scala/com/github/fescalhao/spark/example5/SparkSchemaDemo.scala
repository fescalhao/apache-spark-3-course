package com.github.fescalhao.spark.example5


import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object SparkSchemaDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config(getSparkConf("Spark Schema Demo"))
      .getOrCreate()

    // Works for both CSV and JSON in this example
    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))

    // Works for both CSV and JSON in this example
    val flightSchemaDDL = "FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, " +
      "ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, " +
      "WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"

    val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "data/flight-*.csv")
      .option("mode", "FAILFAST")
      .option("dateFormat", "M/d/yyyy")
      .schema(flightSchemaDDL)
      .load()

    flightTimeCsvDF.show(5)
    logger.info("CSV schema: " + flightTimeCsvDF.schema.simpleString)

    val flightTimeJsonDF = spark.read
      .format("json")
      .option("path", "data/flight-*.json")
      .option("mode", "FAILFAST")
      .option("dateFormat", "M/d/yyyy")
      .schema(flightSchemaStruct)
      .load()

    flightTimeJsonDF.show(5)
    logger.info("JSON schema: " + flightTimeJsonDF.schema.simpleString)

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight-*.parquet")
      .load()

    flightTimeParquetDF.show(5)
    logger.info("Parquet schema: " + flightTimeParquetDF.schema.simpleString)

    spark.stop()
  }
}
