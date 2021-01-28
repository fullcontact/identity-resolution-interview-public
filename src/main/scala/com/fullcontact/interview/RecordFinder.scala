package com.fullcontact.interview

import org.apache.spark.sql.SparkSession

import IOUtil._
import DataFrameUtil._

object RecordFinder {

  val parallelism = 4
  val workers = "*"

  val OUTPUT1_PATH = "./Output1.txt"
  val OUTPUT2_PATH = "./Output2.txt"

  implicit val sparkSession = initSpark(workers)

  def main(args: Array[String]): Unit = {

    assert (verifyInputParameters(args))

    val queryPath = args(0)
    val recordPath = args(1)

    assert(verifyInputFileExists(queryPath, "Queries file"))
    assert(verifyInputFileExists(recordPath, "Records file"))


    val queries = readQueries(queryPath)
    val records = readRecords(recordPath)

    args.length match {
      // broadcast inner loop join  (join condition "array contains") - slower
      case 3 => {

        val joined = joinQueryWithRecord(queries.repartition(parallelism), records.repartition(parallelism))

        saveOutput(produceOutput1(joined), OUTPUT1_PATH)
        saveOutput(produceOutput2(joined), OUTPUT2_PATH)
      }
      // broadcast hash join - faster
      case 2 => {

        val explodedZippedRecords = explodeZippedRecords(records)
        var joined = joinQueryWithRecordExploded(queries, explodedZippedRecords)

        saveOutput(produceOutput1Exploded(joined), OUTPUT1_PATH)
        saveOutput(produceOutput2Exploded(joined), OUTPUT2_PATH)

      }
    }

  }


  private def verifyInputParameters(args: Array[String]): Boolean = {
    if (args == null || args.length < 2) {
      print("Two mandatory arguments have to be passed: \n1. Queries file path \n2. Records file path")
      return false
    }
    true
  }

  private def initSpark(scaleValue: String = "*") = {
    val session = SparkSession.builder().appName("IdentifiersFinder")
      .master(s"local[$scaleValue]")
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
    session
  }
}
