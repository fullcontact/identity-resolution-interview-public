package com.fullcontact.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object RecordFinder extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = new SparkSessionBuilder("RecordFinder").sparkSession

  import spark.implicits._

  val schema = new StructType()
    .add("Id", StringType, nullable = false )

  val queriesDF = spark.read
    .option("sep", " ")
    .schema(schema)
    .csv("src/main/data/Queries.txt")

  val recordsDF = spark.read.text("src/main/data/Records.txt")

  val resultDF = recordsDF
    .transform(convertRecordsToArrayAndThenExplode)
    .transform(getRecordsForQuery(queriesDF))

  resultDF.cache()

  resultDF.transform(getAllRecords).coalesce(1).write.mode(SaveMode.Overwrite).text("src/main/data/output_1/")
  resultDF.transform(getDedupRecords).coalesce(1).write.mode(SaveMode.Overwrite).text("src/main/data/output_2/")

  def convertRecordsToArrayAndThenExplode(df: DataFrame): DataFrame = {
    df
      .withColumn(
        "record", split($"value"," ")
      )
      .select($"value", $"record", explode($"record"))
      .withColumnRenamed("col", "Id")
  }

  def getRecordsForQuery(queriesDF: DataFrame)(recordsDF: DataFrame): DataFrame = {
    recordsDF.join(queriesDF, recordsDF("Id") === queriesDF("Id"), "inner")
      .select($"value", queriesDF("Id"), $"record")
  }

  def getAllRecords(df: DataFrame): Dataset[String] = {
    val output1DF: Dataset[String] = df.map(
      row => s"${row.getString(1)}:${row.getString(0)}"
    )
    output1DF
  }

  def getDedupRecords(df: DataFrame): Dataset[String] = {
    val output2DF = df
      .groupBy("Id")
      .agg(collect_set("record").as("combinedRecords"))
      .withColumn("dedupRecord", concat_ws(" ", array_distinct(flatten($"combinedRecords"))))
      .map(
        row => s"${row.getString(0)}:${row.getString(2)}"
      )
    output2DF
  }
}
