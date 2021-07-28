package com.fullcontact.interview
import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.reflect.io.Directory

object RecordFinder {

  def main(args: Array[String]): Unit = {
    // Spark setup/init
    val conf = new SparkConf()
      .setAppName("BradsRecordFinder")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Reading info RDDs from Records.txt file (into arrays of strings) and Queries (into strings)
    val recordsSplitRDD = sc.textFile("./Records.txt")
      .map(l => l.split(" "))
    val queriesRDD = sc.textFile("./Queries.txt")

    // Row counts/validation on input data (are all septuplets of ASCII 65-90? Do we have non-zero row counts?)
    validateRecords(recordsSplitRDD)
    validateQueries(queriesRDD)

    // Generate Output 1 DF that'll be used in both Output 1 text and Output 2 processing/text
    val output1DF = generateOutput1DF(recordsSplitRDD, queriesRDD)

    // Transforming and outputting Output1 and Output2 (clearing a path if necessary for an idempotent output)
    generateOutput1(output1DF)
    generateOutput2(output1DF)
  }

  def validateRecords(records: RDD[Array[String]]) : Unit = {
    val non7UppersInRecords = records.map(sa => areWordsNot7Uppers(sa))
      .map(ia => ia.sum)
      .reduce((a, b) => a + b)
    println("Number of non-7-uppercase-letter words imported from Records.txt: " + non7UppersInRecords)
    println("Number of records in Records.txt: " + records.count())
    if (non7UppersInRecords > 0){
      throw new RuntimeException("Non-7-uppercase-letter records found. Make sure all IDs are 7 uppercase letters.")
    }
    if (records.count() == 0) {
      throw new RuntimeException("No records found from Records.txt. Please check path and data file.")
    }
  }

  def validateQueries(queries: RDD[String]) : Unit = {
    val non7UppersInQueries = queries.map(s => isWordNot7Uppers(s))
      .reduce((a, b) => a + b)
    println("Number of non-7-uppercase-letter words imported from Queries.txt: " + non7UppersInQueries)
    println("Number of records in Queries.txt: " + queries.count())
    if (non7UppersInQueries > 0){
      throw new RuntimeException("Non-7-uppercase-letter queries found. Make sure all IDs are 7 uppercase letters.")
    }
    if (queries.count() == 0) {
      throw new RuntimeException("No records found from Queries.txt. Please check path and data file.")
    }
  }

  def areWordsNot7Uppers(sa: Array[String]) : Array[Int] = {
    val numUpperLetters = sa.map(w => isWordNot7Uppers(w))
    return numUpperLetters
  }

  def isWordNot7Uppers(str: String) : Int = {
    var upperCount = 0
    str.foreach(c => {
      if (c.toInt >= 65 && c.toInt <= 90){
        upperCount += 1
      }
    })
    if (upperCount == 7) {
      return 0
    } else {
      return 1
    }
  }

  def generateOutput1DF(recordsSplitRDD: RDD[Array[String]], queriesRDD: RDD[String]): DataFrame = {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    import spark.implicits._

    // Indexing Records
    val recordsSplitIndexedDF = spark.createDataFrame(recordsSplitRDD.zipWithIndex())
      .withColumnRenamed("_1", "partialNeighborArray")
      .withColumnRenamed("_2", "recordsIndex")

    // Creating a flattened 'bridge' DF of (Record index : ID) to join against Queries
    val recordsIndicesFlatDF = recordsSplitIndexedDF.select($"recordsIndex", explode($"partialNeighborArray"))
      .withColumnRenamed("col", "ID")

    // Bringing Queries into a DF for joining into the tables above.
    val queriesDF = spark.createDataFrame(queriesRDD.zipWithIndex())
      .withColumnRenamed("_1", "ID")
      .withColumnRenamed("_2", "queryIndex")

    // Getting DF version of Output1 (find what - 0:Many - Records array each Query ID was in)
    val output1DF = queriesDF
      .join(recordsIndicesFlatDF, queriesDF("ID") === recordsIndicesFlatDF("ID"), "inner")
      .join(recordsSplitIndexedDF, recordsIndicesFlatDF("recordsIndex") === recordsSplitIndexedDF("recordsIndex"), "inner")
      .select(
        queriesDF("ID") as "ID",
        recordsSplitIndexedDF("partialNeighborArray") as "partialNeighborArray"
      )

    return output1DF
  }

  def generateOutput1(df: DataFrame): Unit = {
    if (Files.exists(Paths.get("./Output1.txt"))){
      val dir1 = new Directory(new File("./Output1.txt"))
      dir1.deleteRecursively()
    }

    df
      .withColumn("partialNeighborString", concat_ws(" ", col("partialNeighborArray")))
      .select("ID", "partialNeighborString")
      .rdd
      .map(_.toString()
        .replace(",", ": ")
        .replace("[", "")
        .replace("]", "")
      )
      .saveAsTextFile("./Output1.txt")
  }

  def generateOutput2(df: DataFrame): Unit = {
    if (Files.exists(Paths.get("./Output2.txt"))){
      val dir2 = new Directory(new File("./Output2.txt"))
      dir2.deleteRecursively()
    }

    df
      .rdd
      .map(row => (row.get(0), row.get(1).asInstanceOf[mutable.WrappedArray[String]].toSet))
      .reduceByKey(_ | _)
      .map(_.toString()
        .replace(",Set", ": ")
        .replace("(", "")
        .replace(")", "")
        .replace(",", " ")
      )
      .saveAsTextFile("./Output2.txt")
  }
}
