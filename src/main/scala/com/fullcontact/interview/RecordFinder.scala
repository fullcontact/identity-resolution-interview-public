package com.fullcontact.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => SQLFunc}

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

    val records: DataFrame = sparkSession.read
      .textFile("Records.txt")
      .map(x => x.split("\\W+"))
      .withColumnRenamed("value", "recordIds")

    val recordsByQuery: DataFrame  = queries.join(records, SQLFunc.array_contains(records("recordIds"), queries("query")))

    recordsByQuery.map(row => (row.getString(0), row.getSeq[String](1).mkString(" ")))
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", ":")
      .csv("Output1.txt")

    val reducedRecordsByQuery: DataFrame = recordsByQuery.groupBy($"query")
      .agg(
        SQLFunc.concat_ws(" ",
          SQLFunc.array_distinct(
            SQLFunc.flatten(
              SQLFunc.collect_list($"recordIds")
            )
          )
        )
      )

    reducedRecordsByQuery.write
      .mode(SaveMode.Overwrite)
      .option("delimiter", ":")
      .csv("Output2.txt")
  }
}
