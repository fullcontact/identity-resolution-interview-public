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
      .setMaster("local[4]")
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

    // Transforming and outputting Output1 and Output2
    val output1RDD = generateFinalOutput1RDD(output1DF)
    val output2RDD = generateFinalOutput2RDD(output1DF)

    saveOutputText(output1RDD, "./Output1.txt")
    saveOutputText(output2RDD, "./Output2.txt")
  }

  def validateRecords(records: RDD[Array[String]]) : Boolean = {
    val non7UppersInRecords = records.map(sa => areWordsNot7Uppers(sa))
      .map(ia => ia.sum)
      .reduce((a, b) => a + b)
    if (non7UppersInRecords > 0){
      throw new RuntimeException("Non-7-uppercase-letter records found. Make sure all IDs are 7 uppercase letters.")
    }
    if (records.count() == 0) {
      throw new RuntimeException("No records found from Records.txt. Please check path and data file.")
    }
    true
  }

  def validateQueries(queries: RDD[String]) : Boolean = {
    val non7UppersInQueries = queries.map(s => isWordNot7Uppers(s))
      .reduce((a, b) => a + b)
    if (non7UppersInQueries > 0){
      throw new RuntimeException("Non-7-uppercase-letter queries found. Make sure all IDs are 7 uppercase letters.")
    }
    if (queries.count() == 0) {
      throw new RuntimeException("No records found from Queries.txt. Please check path and data file.")
    }
    true
  }

  def areWordsNot7Uppers(sa: Array[String]) : Array[Int] = {
    sa.map(w => isWordNot7Uppers(w))
  }

  def isWordNot7Uppers(str: String) : Int = {
    var upperCount = 0
    str.foreach(c => {
      if (c.toInt >= 65 && c.toInt <= 90){
        upperCount += 1
      }
    })
    if (upperCount == 7) 0 else 1
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

    // Returning  DF version of Output1 (find what - 0:Many - Records array each Query ID was in)
    queriesDF
      .join(recordsIndicesFlatDF, queriesDF("ID") === recordsIndicesFlatDF("ID"), "inner")
      .join(recordsSplitIndexedDF, recordsIndicesFlatDF("recordsIndex") === recordsSplitIndexedDF("recordsIndex"), "inner")
      .select(
        queriesDF("ID") as "ID",
        recordsSplitIndexedDF("partialNeighborArray") as "partialNeighborArray"
      )
  }

  def generateFinalOutput1RDD(df: DataFrame): RDD[String] = {
    df
      .withColumn("partialNeighborString", concat_ws(" ", col("partialNeighborArray")))
      .select("ID", "partialNeighborString")
      .rdd
      .map(_.toString()
        .replace(",", ": ")
        .replace("[", "")
        .replace("]", "")
      )
  }

  def generateFinalOutput2RDD(df: DataFrame): RDD[String] = {
    df.rdd
      .map(row => (row.get(0), row.get(1).asInstanceOf[mutable.WrappedArray[String]].toSet))
      .reduceByKey(_ | _)
      .map(_.toString()
        .replace(",Set", ": ")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
      )
  }

  def saveOutputText(rdd: RDD[String], path: String): Unit = {
    // clear a path if necessary, then output RDD to text
    if (Files.exists(Paths.get(path))){
      val dir = new Directory(new File(path))
      dir.deleteRecursively()
    }
    rdd.saveAsTextFile(path)
  }
}
