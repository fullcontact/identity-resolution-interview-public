package com.fullcontact.interview
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RecordFinder {
  def main(args: Array[String]): Unit = {
    // Spark setup/init
    val conf = new SparkConf()
      .setAppName("BradsRecordFinder")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Reading info RDDs from Records.txt file (into arrays of strings) and Queries (into strings)
    val recordsSplitRDD = sc.textFile("Records.txt")
      .map(l => l.split(" "))
    val queriesRDD = sc.textFile("Queries.txt")

    // Row counts/validation on input data (are all septuplets of ASCII 65-90? Do we have non-zero row counts?)
    validateRecords(recordsSplitRDD)
    validateQueries(queriesRDD)

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


}
