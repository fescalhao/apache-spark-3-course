package com.github.fescalhao.spark.example13

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
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
      StructField("InvoiceDate", DateType),
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
      .option("dateFormat", "dd-MM-yyyy H.mm") // 14-08-2011 9.57
      .schema(invoicesSchema)
      .load()

    logger.info("Repartitioning and Caching invoices dataframe")
    val cachedInvoiceDF = invoicesDF.select("InvoiceNo", "Quantity", "InvoiceDate", "UnitPrice", "Country").cache()

    logger.info("Aggregation with Column Expression")
    cachedInvoiceDF.select(
      count("*") as "TotalRecords",
      sum("Quantity") as "TotalQuantity",
      avg("UnitPrice") as "AvgPrice",
      countDistinct("InvoiceNo") as "CountDistinct"
    )
      .show()

    logger.info("Aggregation with SQL Expression")
    cachedInvoiceDF.selectExpr(
      "count(1) as `TotalRecordsExpr`",
      "count(InvoiceNo) as `TotalStockCodeExpr`",
      "sum(Quantity) as `TotalQuantityExpr`"
    )
      .show()

    logger.info("Aggregation with Column Expression and Group By")
    cachedInvoiceDF.groupBy("Country", "InvoiceNo")
      .agg(
        sum("Quantity") as "TotalQuantity",
        round(sum(expr("UnitPrice * Quantity")), 2) as "InvoiceValue"
      )
      .show()

    logger.info("Aggregation with SQL Expression and TempView")
    cachedInvoiceDF.createOrReplaceTempView("vw_Invoices")

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
    val summaryInvoicesDF = cachedInvoiceDF
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy("Country", "WeekNumber")
      .agg(
        count("InvoiceNo") as "NumInvoices",
        sum("Quantity") as "TotalQuantity",
        round(sum(expr("UnitPrice * Quantity")), 2) as "InvoiceValue"
      )

    summaryInvoicesDF.coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/parquet/")
      .save()

    summaryInvoicesDF.sort("Country", "WeekNumber").show()

    val runningTotalWindow = Window
      .partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryInvoicesDF.withColumn("RunningTotal",
      round(sum("InvoiceValue").over(runningTotalWindow), 2)
    )
      .show()

    logger.info("Cleaning invoice dataframe cache...")
    cachedInvoiceDF.unpersist()

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}