package com.fullcontact.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

abstract class SparkTest extends FunSuite with BeforeAndAfter {

  val master = "local[*]"
  val enableUI = false

  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _

  before {
    sparkConf = new SparkConf(true).setMaster(master).setAppName("recordFinderTest").set("spark.ui.enabled", enableUI.toString)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  }

}
