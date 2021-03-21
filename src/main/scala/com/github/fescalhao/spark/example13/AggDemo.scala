package com.github.fescalhao.spark.example13

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}

object AggDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Agg Demo"))
      .getOrCreate()

    logger.info("Defining invoice Schema...")
    val invoicesSchema = StructType(List(
      StructField("InvoiceNo", StringType),
      StructField("StockCode", StringType),
      StructField("Description", StringType),
      StructField("Quantity", IntegerType),
      StructField("InvoiceDate", StringType),
      StructField("UnitPrice", FloatType),
      StructField("CustomerID", IntegerType),
      StructField("Country", StringType)
    ))

    logger.info("Loading invoices file...")
    val invoicesDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "data/invoices.csv")
      .option("treatEmptyValuesAsNulls", "true")
      .option("nullValue", null)
      .schema(invoicesSchema)
      .load()

    logger.info("Caching invoices dataframe")
    invoicesDF.cache()

    logger.info("Aggregation with Column Expression")
    invoicesDF.select(
      count("*") as "TotalRecords",
      sum("Quantity") as "TotalQuantity",
      avg("UnitPrice") as "AvgPrice",
      countDistinct("InvoiceNo") as "CountDistinct"
    )
      .show()

    logger.info("Aggregation with SQL Expression")
    invoicesDF.selectExpr(
      "count(1) as `TotalRecordsExpr`",
      "count(InvoiceNo) as `TotalStockCodeExpr`",
      "sum(Quantity) as `TotalQuantityExpr`"
    )
      .show()

    logger.info("Aggregation with Column Expression and Group By")
    invoicesDF.groupBy("Country", "InvoiceNo")
      .agg(
        sum("Quantity") as "TotalQuantity",
        round(sum(expr("UnitPrice * Quantity")), 2) as "InvoiceValue"
      ).show()

    logger.info("Aggregation with SQL Expression and TempView")
    invoicesDF.createOrReplaceTempView("vw_Invoices")

    spark.sql(
      """
        |select Country, InvoiceNo,
        |       sum(Quantity) as `TotalQuantity`,
        |       round(sum(Quantity * UnitPrice), 2) as `InvoiceValue`
        |   from vw_Invoices
        |  group by Country, InvoiceNo
        |""".stripMargin)
      .show()

    logger.info("Exercise...")
    invoicesDF.withColumn("InvoiceDate", to_date(regexp_replace(col("InvoiceDate"), "( \\d+.\\d+)", ""), "dd-MM-yyyy"))
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy("Country", "WeekNumber")
      .agg(
        count("InvoiceNo") as "NumInvoices",
        sum("Quantity") as "TotalQuantity",
        round(sum(expr("UnitPrice * Quantity")), 2) as "InvoiceValue"
      )
      .show()

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}