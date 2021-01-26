package com.fullcontact.interview

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array_contains, array_distinct, col, collect_list, concat_ws, flatten, sort_array, split}

object RecordFinder {
  def main(args: Array[String]): Unit = {

    if (args == null || args.length < 2) {
      print("Two mandatory arguments have to be passed: 1. Queries file path 2. Records file path")
      return
    }

    val queryPath = args(0)
    val recordPath = args(1)

    verifyInputFileExists(queryPath, "Queries file")
    verifyInputFileExists(recordPath, "Records file")

    val sparkConf: SparkConf = buildSparkConf
    implicit val sparkSession = initSpark(sparkConf)

    val queries = readQueries(queryPath)
    val records = readRecords(recordPath)
    val joined = joinQueryWithRecord(queries, records)

    produceOutput1(joined)
    produceOutput2(joined)

  }

  private def verifyInputFileExists(queryPath: String, messagePrefix: String) = {
    try {
      if (!new File(queryPath).exists()) {
        print(s"$messagePrefix: $queryPath, does not exists")
      }
    } catch {
      case e:Exception => print(e.getMessage)
    }
  }

  private def joinQueryWithRecord(queries: DataFrame, records: DataFrame) = {
    val joined = queries.join(right = records, array_contains(col("identifiers_arr"), col("identifier"))).repartition(5)
    joined.cache()
    joined
  }

  private def produceOutput2(joined: Dataset[Row]) = {
    joined
      .groupBy("identifier")
      .agg(collect_list("identifiers_arr").alias("identifiers_arr"))
      .withColumn("identifiers_arr", concat_ws(" ", sort_array(array_distinct(flatten(col("identifiers_arr"))))))
      .drop("identifiers")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      //      .option("header", "false")
      .option("delimiter", ":")
      .csv("./Output2.txt")
  }

  private def produceOutput1(joined: Dataset[Row]) = {
    joined.withColumn("identifiers_arr", concat_ws(" ", col("identifiers")))
      .drop("identifiers")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", ":")
      .csv("./Output1.txt")
  }

  private def readRecords(recordPath: String)(implicit sparkSession: SparkSession) = {
    val records = sparkSession.read
      .option("header", false)
      .option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "corrupt_record")
      .csv(recordPath)
      .withColumnRenamed("_c0", "identifiers")
      .withColumn("identifiers_arr", split(col("identifiers"), "\\W"))
    records
  }

  private def readQueries(queryPath: String)(implicit sparkSession: SparkSession) = {
    sparkSession.read
      .option("header", false)
      .option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "corrupt_record")
      .csv(queryPath)
      .withColumnRenamed("_c0", "identifier")
  }

  private def initSpark(sparkConf: SparkConf) = {
    SparkSession.builder().appName("IdentifiersFinder")
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()
  }

  private def buildSparkConf = {
    val sparkConf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.executor.cores", "4")
      .set("spark.dynamicAllocation.minExecutors", "1")
      .set("spark.dynamicAllocation.maxExecutors", "5")
      .set("dynamicAllocation.enabled", "true")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "2g")
      .set("spark.hadoop.validateOutputSpecs", "false")
    sparkConf
  }
}
