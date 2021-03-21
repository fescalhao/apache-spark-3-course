package com.github.fescalhao.spark.example14

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Spark Join Demo"))
      .getOrCreate()

    logger.info("Defining the order list")
    val ordersList = List(
      ("01", "02", 350, 1),
      ("01", "04", 580, 1),
      ("01", "07", 320, 2),
      ("02", "03", 450, 1),
      ("02", "06", 220, 1),
      ("03", "01", 195, 1),
      ("04", "09", 270, 3),
      ("04", "08", 410, 2),
      ("05", "02", 350, 1)
    )

    logger.info("Creating the order dataframe")
    val orderDF = spark.createDataFrame(ordersList).toDF("order_id", "prod_id", "unit_price", "qty")
      .withColumnRenamed("qty", "order_qty")
      .withColumnRenamed("prod_id", "prod_id_fk")

    logger.info("Defining the product list")
    val productList = List(
      ("01", "Scroll Mouse", 250, 20),
      ("02", "Optical Mouse", 350, 20),
      ("03", "Wireless Mouse", 450, 50),
      ("04", "Wireless Keyboard", 580, 50),
      ("05", "Standard Keyboard", 360, 10),
      ("06", "16 GB Flash Storage", 240, 100),
      ("07", "32 GB Flash Storage", 320, 50),
      ("08", "64 GB Flash Storage", 430, 25)
    )

    logger.info("Creating the product dataframe")
    val productDF = spark.createDataFrame(productList).toDF("prod_id", "prod_name", "list_price", "qty")
      .withColumnRenamed("qty", "prod_qty")

    logger.info("Joining both dataframes...")
    val joinExpr = orderDF.col("prod_id_fk") === productDF.col("prod_id")
    orderDF.join(productDF, joinExpr, "left")
      .select("order_id", "prod_name", "unit_price", "order_qty")
      .show()


    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}
