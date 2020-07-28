/*
Author: FullContact
Modified By: Jeff Willingham
Creation Date: unknown
Modification Date: 2020-07-28
Purpose: This script inputs Queries.txt and Records.txt, performs transformations and writes outputs per specifications outlined in README.md.
*/

package com.fullcontact.interview

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RecordFinder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession // instantiate spark session
      .builder()
      .appName("RecordFinder")
      .master("local[*]")
      .getOrCreate
    val queriesDF: DataFrame = spark // read in Queries.txt as a DataFrame
      .read
      .format("text")
      .load("Queries.txt")
      .withColumnRenamed("value", "queryID")
    val recordsDF: DataFrame = spark // read in Records.txt as a DataFrame, and create two fields
      .read
      .format("text")
      .load("Records.txt")
      .withColumn("idArray", split(col("value"), "\\s"))
      .withColumn("idArrayExploded", explode(col("idArray")))
    queriesDF // join inputs to create and write Output1.txt
      .join(recordsDF, col("queryID") === col("idArrayExploded"), "inner")
      .withColumn("output1", concat_ws(":", col("queryID"), col("value")))
      .select("output1")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("text")
      .save("Output1.txt")
    queriesDF // join inputs to create and write Output2.txt
      .join(recordsDF, col("queryID") === col("idArrayExploded"), "inner")
      .groupBy("queryID")
      .agg(collect_set("value") as "value")
      .withColumn("value", concat_ws(" ", col("value")))
      .withColumn("value", split(col("value"), "\\s"))
      .withColumn("value", explode(col("value")))
      .groupBy("queryID")
      .agg(collect_set("value") as "value")
      .withColumn("value", concat_ws(" ", col("value")))
      .withColumn("output2", concat_ws(":", col("queryID"), col("value")))
      .select("output2")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("text")
      .save("Output2.txt")
  }
}