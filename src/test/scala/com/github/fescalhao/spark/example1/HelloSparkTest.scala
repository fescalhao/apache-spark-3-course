package com.github.fescalhao.spark.example1

import com.github.fescalhao.spark.example1.HelloSpark.{countByCountry, loadSampleDF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class HelloSparkTest extends FunSuite with BeforeAndAfterAll {
  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .config(getTestSparkConf)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading") {
    val sampleDF = loadSampleDF(spark, "data/sample.csv")
    val sampleCount = sampleDF.count()
    assert(sampleCount == 9, " record count should be 9")
  }

  test("Group By Country") {
    val sampleDF = loadSampleDF(spark, "data/sample.csv")
    val countCountry = countByCountry(sampleDF)
    val countryMap = new mutable.HashMap[String, Long]
    countCountry.collect().foreach(r => countryMap.put(r.getString(0), r.getLong(1)))

    assert(countryMap("United Kingdom") == 1, ":- Count for United Kingdom should be 1")
    assert(countryMap("Canada") == 2, ":- Count for Canada should be 2")
    assert(countryMap("United States") == 4, ":- Count for United States should be 4")
  }

  def getTestSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("HelloSparkTest")
      .setMaster("local[*]")
  }
}
