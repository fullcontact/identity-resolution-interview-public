package com.fullcontact.interview

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object RecordFinder {
  val Identifier = "identifier"
  val Query = "query"
  val Record = "record"
  val Records = "records"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .getOrCreate()

    val records = spark.read.csv("./Records.txt").toDF(Record).repartition(4)
    val queries = spark.read.csv("./Queries.txt").toDF(Query).repartition(4)

    val recordsById = explodeRecordsDF(records)

    val withDupes = recordsById.join(queries, recordsById(Identifier) === queries(Query))
      .select(queries(Query), recordsById(Record))

    import spark.sqlContext.implicits._
    withDupes
      .map(row => {
        s"${row.getAs(Query)}: ${row.getAs(Record)}"
      })
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .text("Output1.txt")

    val noDupes = groupRecordsByIdDF(withDupes)

    noDupes
      .map(row => s"${row.getAs(Identifier)}: ${row.getAs(Records)}")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .text("Output2.txt")
  }

  //(a b c) -> (a, a b c), (b, a b c), (c, a b c)
  def explodeRecordsDF(recordsDF: DataFrame): DataFrame = {
    import recordsDF.sqlContext.implicits._
    recordsDF.as[String].mapPartitions(rows => {
      val exploded = rows.flatMap(record => {
        val identifiers: Array[String] = record.split(" ")
        identifiers.map(id => (id, record))
      })
      exploded
    }).toDF(Identifier, Record)
  }

  //(a, a b c), (a, c e f) -> (a, a b c e f)
  def groupRecordsByIdDF(duped: DataFrame): DataFrame = {
    import duped.sqlContext.implicits._
    duped
      .map(row => (row.getString(0), row.getString(1).split(" ")))
      .toDF(Identifier, Records)
      .groupBy(Identifier)
      .agg(concat_ws(" ", array_distinct(flatten(collect_list(col(Records))))).alias(Records)
      )
  }


}


