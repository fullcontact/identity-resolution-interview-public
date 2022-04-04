package com.fullcontact.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RecordFinder {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Record Builder")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val queries: DataFrame = sparkSession.read
      .textFile("Queries.txt")
      .withColumnRenamed("value", "query")

    queries.show(5, false)
  }
}
