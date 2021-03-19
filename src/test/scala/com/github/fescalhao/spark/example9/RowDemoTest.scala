package com.github.fescalhao.spark.example9

import com.github.fescalhao.spark.example8.RowDemo.toDateDF
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.sql.Date

class RowDemoTest extends FunSuite with BeforeAndAfterAll {
  @transient var spark: SparkSession = _
  @transient var myDF: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .config(getTestSparkConf)
      .getOrCreate()

    val mySchema = StructType(List(
      StructField("ID", StringType),
      StructField("EventDate", StringType)
    ))

    val myRows = List(
      Row("1", "01/05/2020"),
      Row("2", "1/5/2020"),
      Row("3", "01/5/2020"),
      Row("4", "1/05/2020")
    )

    val myRDD = spark.sparkContext.parallelize(myRows, 2)
    myDF = spark.createDataFrame(myRDD, mySchema)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Test Data Type") {
    val rowList = toDateDF(myDF, "M/d/y", "EventDate").collectAsList()
    rowList.forEach(row => {
      assert(row.get(1).isInstanceOf[Date], "Second column should be of Date type")
    })
  }

  test("Test Data Value") {
    val spark2 = spark
    import spark2.implicits._
    val rowList = toDateDF(myDF, "M/d/y", "EventDate").as[EventRecord].collectAsList()
    rowList.forEach(row => {
      assert(row.EventDate.toString == "2020-01-05", "Date string must be 2020-01-05")
    })
  }

  def getTestSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("RowDemoTest")
      .setMaster("local[*]")
  }

}
