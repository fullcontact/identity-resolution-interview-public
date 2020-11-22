package com.fullcontact.interview

import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import com.holdenkarau.spark.testing.DataFrameSuiteBase

class RecordFinderTest extends FunSuite with Matchers with DataFrameSuiteBase {
  test("makeRecordMap") {
    //val spark: SparkSession = SparkSession.builder.getOrCreate()
//    import spark.implicits._

    val input = List("A B C", "E F G")
//    val records: RDD[String] = spark.sparkContext.parallelize(input)

//    val rdd: RDD[(String, Set[String])] = RecordFinder.makeRecordMap(records)
//    rdd.take(2).foreach(x => println(s"***$x"))

    assert(true)
  }
}

