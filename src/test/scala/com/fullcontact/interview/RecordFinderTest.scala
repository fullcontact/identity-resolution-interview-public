package com.fullcontact.interview

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{Assertion, FunSuite, Matchers}
class RecordFinderTest extends FunSuite with Matchers {

  test("Test RDD results") {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val testQueryPath = "./src/test/resources/mockedQueries.txt"
    val testRecordsPath = "./src/test/resources/mockedRecords.txt"

    val (joined_RDD, dedupe_RDD) = RecordFinder.findResolvedRDDs(sparkSession, testQueryPath, testRecordsPath)

    joinedRDDValidate(joined_RDD)
    distinctRDDValidate(dedupe_RDD)
  }

  def joinedRDDValidate(rdd:  RDD[(String, String)]): Array[Assertion] = {

    rdd.collect().map{case (key, values) =>
      key match {
        case "AAAAAAA" =>
          assert(values=="AAAAAAA : AAAAAAA CCCCCCC" || values=="AAAAAAA : BBBBBBB AAAAAAA AAAAAAA" || values=="AAAAAAA : BBBBBBB AAAAAAA AAAAAAA")
        case "BBBBBBB" =>
          assert(values=="BBBBBBB : BBBBBBB AAAAAAA AAAAAAA")
        case "CCCCCCC" =>
          assert(values=="CCCCCCC : AAAAAAA CCCCCCC"||values=="CCCCCCC : CCCCCCC VVVVVV")
        case "DDDDDDD" =>
          assert(values=="DDDDDDD : bbbbbbb DDDDDDD")
        case "ZZZZZZZ" =>
          assert(values=="ZZZZZZZ : ")
        case _ =>
          assert(false)
      }
    }
  }

  def distinctRDDValidate(rdd:  RDD[(String, String)]): Array[Assertion] = {
    rdd.collect().map{ case (key, values) =>
      key match {
        //all values previously sorted in caller
        case "AAAAAAA" =>
          assert(values == "AAAAAAA : AAAAAAA CCCCCCC BBBBBBB")
        case "BBBBBBB" =>
          assert(values=="BBBBBBB : BBBBBBB AAAAAAA")
        case "CCCCCCC" =>
          assert(values=="CCCCCCC : AAAAAAA CCCCCCC")
        case "DDDDDDD" =>
          assert(values == "DDDDDDD : DDDDDDD")
        case "ZZZZZZZ" =>
          assert(values == "ZZZZZZZ : ")
        case _ =>
          assert(false)
      }
    }
  }
}
