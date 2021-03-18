package com.github.fescalhao.spark.example6

import com.github.fescalhao.spark.example6.DataSinkDemo.countCancelledFlightsByDistance
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DataSinkDemoTest extends FunSuite with BeforeAndAfterAll {
  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .config(getTestSparkConf)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Count Cancelled Flights by Origin and Distance") {
    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", s"data/flight-*.parquet")
      .load()

    val partitionedDF = flightTimeParquetDF.repartition(2)
    val cancelledFlightsDF = countCancelledFlightsByDistance(partitionedDF)

    val cancelledFlightNum: Int = cancelledFlightsDF.where("origin_city == 'Baltimore, MD' and distance == 220")
      .select("cancelled_flights")
      .collect()
      .head
      .getInt(0)

    assert(cancelledFlightNum == 15, "Count for Baltimore, MD with distance of 220 should be 15")
  }

  def getTestSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("DataSinkDemoTest")
      .setMaster("local[*]")
  }
}
