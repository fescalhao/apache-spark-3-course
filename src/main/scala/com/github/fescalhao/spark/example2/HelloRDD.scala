package com.github.fescalhao.spark.example2

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object HelloRDD extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      logger.info("Usage: HelloRDD filename")
      System.exit(1)
    }

    val sparkContext = new SparkContext(getSparkConf("Hello RDD"))

    val linesRDD = sparkContext.textFile(args(0))
    val partitionedRDD = linesRDD.repartition(2)

    val colsRDD = partitionedRDD.map(line => line.split(",").map(_.trim))
    val selectRDD = colsRDD.map(cols => SampleRDDRecord(cols(1).toInt, cols(2), cols(3), cols(4)))
    val filterRDD = selectRDD.filter(row => row.Age < 40)

    val kvRDD = filterRDD.map(row => (row.Country, 1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

    logger.info(countRDD.collect().mkString(","))

    sparkContext.stop()
  }
}
