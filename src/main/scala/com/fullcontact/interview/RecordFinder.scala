package com.fullcontact.interview
import org.apache.spark.{SparkConf, SparkContext}

object RecordFinder {
  def main(args: Array[String]): Unit = {
    // Spark setup/init
    val conf = new SparkConf()
      .setAppName("BradsRecordFinder")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Reading info from Records.txt file (into arrays of strings) and Queries (into strings)
    val recordsSplitRDD = sc.textFile("Records.txt")
      .map(l => l.split(" "))

    val queriesRDD = sc.textFile("Queries.txt")

    // Do some stats/validation on input data (are all septuplets of ASCII 65-90?)
    val non7UppersInRecords = recordsSplitRDD.map(sa => areWordsNot7Uppers(sa))
      .map(ia => ia.sum)
      .reduce((a, b) => a + b)
    println("Number of non-7-uppercase-letter words imported from Records.txt: " + non7UppersInRecords)
    println("Number of records in Records.txt: " + recordsSplitRDD.count())

    val non7UppersInQueries = queriesRDD.map(s => isWordNot7Uppers(s))
      .reduce((a, b) => a + b)
    println("Number of non-7-uppercase-letter words imported from Queries.txt: " + non7UppersInQueries)
    println("Number of records in Queries.txt: " + queriesRDD.count())
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
