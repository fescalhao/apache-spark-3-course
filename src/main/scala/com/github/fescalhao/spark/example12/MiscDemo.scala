package com.github.fescalhao.spark.example12

import com.github.fescalhao.SparkConfigUtils.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import functions._
import org.apache.spark.sql.types.IntegerType

object MiscDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating Spark Session...")
    val spark = SparkSession.builder()
      .config(getSparkConf("Misc Demo"))
      .getOrCreate()

    logger.info("Creating data list...")
    val dataList = List(
      ("Arya", "15", "8", "91"), // 1991
      ("Carl", "10", "6", "20"), // 2020
      ("Arya", "15", "8", "91"), // 1991
      ("Paul", "4", "12", "6"), // 2006
      ("John", "7", "5", "2002") // 2002
    )

    logger.info("Creating dataframe, setting column names and repartitioning")
    val rawDF = spark.createDataFrame(dataList).toDF("name", "day", "month", "year").repartition(2)

    logger.info("Adding a monotonically id (unique but not sequential)")
    logger.info("Adding a 'case' expression to correct the year column values")
    val finalDF = rawDF.withColumn("id", monotonically_increasing_id())
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      /*
      .withColumn("year", expr(
        """
          |case
            |when year < 21 then year + 2000
            |when year < 100 then year + 1900
            |else year
          |end
          |""".stripMargin))
       */
      .withColumn("year",
        when(col("year") < 21, col("year") + 2000)
        when(col("year") < 100, col("year") + 1900)
        otherwise col("year")
      )
      .withColumn("birthday", to_date(expr("concat(day, '/', month, '/', year)"), "d/M/y"))
      .drop("day", "month", "year")
      .dropDuplicates("name", "birthday")
      .sort(expr("birthday desc"))

    finalDF.show()

    logger.info("Stopping Spark...")
    spark.stop()
    logger.info("Spark stopped")
  }
}
