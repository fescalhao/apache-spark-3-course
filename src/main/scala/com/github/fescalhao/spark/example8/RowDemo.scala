package com.github.fescalhao.spark.example8

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RowDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Row Demo"))
      .getOrCreate()

    logger.info("Creating a Dataframe Schema...")
    val mySchema = StructType(List(
      StructField("ID", StringType),
      StructField("EventDate", StringType)
    ))

    logger.info("Creating rows for Dataframe...")
    val myRows = List(
      Row("1", "01/05/2018"),
      Row("2", "2/5/2019"),
      Row("3", "03/5/2020"),
      Row("4", "4/05/2021")
    )

    logger.info("Creating RDD with 2 partitions...")
    val myRDD = spark.sparkContext.parallelize(myRows, 2)
    logger.info("Creating Dataframe...")
    val myDF = spark.createDataFrame(myRDD, mySchema)

    logger.info("Printing Dataframe before transformation...")
    myDF.printSchema()
    myDF.show()

    val newDF = toDateDF(myDF, "M/d/y", "EventDate")

    logger.info("Printing Dataframe after transformation...")
    newDF.printSchema()
    newDF.show()

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }

  def toDateDF(df: DataFrame, fmt: String, column: String): DataFrame = {
    df.withColumn(column, to_date(col(column), fmt))
  }
}
