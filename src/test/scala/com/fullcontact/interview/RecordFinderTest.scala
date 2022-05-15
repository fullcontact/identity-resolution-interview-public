package com.fullcontact.interview

import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



class RecordFinderTest extends FunSuite with Matchers {

  val config = new SparkConf()
  config.set("spark.debug.maxToStringFields", "10000")


  // create the SparkSession
  val builder = SparkSession.builder()
    .appName("RecordFinder")
    .config(config)


  // use only local mode
  implicit val sparkSession = builder.master("local[*]").getOrCreate()

  test("Basic Case") {

    import sparkSession.implicits._

    val recordsDF = Seq(
      "AAA BBB CCC", //should be returned twice
      "BBB CCC DDD", //should be returned twice
      "DDD" // record should never be returned
    ).toDF("idList")


    val queriesDf = Seq(
      "BBB", //in first two lines
      "CCC", //in first two lines
      "FFF" // no results
    ).toDF("queryId")


    val recordFinder = new RecordFinder
    val (results1Df, results2Df) = recordFinder.findResults(recordsDF, queriesDf)

    val results1 = results1Df.collect()
    val results2 = results2Df.collect()

    println(results1.mkString("Array(", ", ", ")"))
    println(results2.mkString("Array(", ", ", ")"))

    results1 should equal (Array("BBB: AAA BBB CCC", "BBB: BBB CCC DDD", "CCC: AAA BBB CCC", "CCC: BBB CCC DDD"))
    results2 should equal (Array("CCC: AAA BBB CCC DDD", "BBB: AAA BBB CCC DDD"))
  }
}
