package com.github.fescalhao

import org.apache.spark.SparkConf

import java.util.Properties
import scala.io.Source

package object SparkConfigUtils {
  def getSparkConf(appName: String): SparkConf = {
    val sparkConf = new SparkConf()
    val props = getSparkConfProperties
    props.forEach((k,v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf.setAppName(appName)
  }

  private def getSparkConfProperties: Properties = {
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())

    props
  }
}
