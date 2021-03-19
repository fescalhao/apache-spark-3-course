package com.github.fescalhao.spark.example9

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.substring_index

object LogFileDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val fileName = "apache_logs.txt"

    logger.info("Creating Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Log File Demo"))
      .getOrCreate()

    logger.info(s"Reading $fileName file")
    val logDF = spark.read
      .format("text")
      .option("path", s"data/$fileName")
      .load()

    val logRegx = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

    logger.info("Extracting required fields with map")
    import spark.implicits._
    val logMapDF = logDF.map(row => {
      row.getString(0) match {
        case logRegx (ip, client, user, date, cmd, request, proto, status, bytes, referrer, userAgent) => LogRow(ip, date, request, referrer)
      }
    })

    logger.info("Cleaning, Grouping and Counting by column 'referrer'")
    logMapDF.where("trim(referrer) != '-'")
      .withColumn("referrer", substring_index($"referrer", "/", 3))
      .groupBy("referrer")
      .count()
      .show(false)

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}
