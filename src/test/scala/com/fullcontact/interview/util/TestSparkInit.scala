package com.fullcontact.interview.util

import org.apache.spark.sql.SparkSession

class TestSparkInit extends SparkInit {
  override def createSession(): SparkSession = {

    val testConfiguration = minimalConf
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.kryo.registrationRequired", "false")

    val sparkSession = SparkSession.builder().config(testConfiguration)
      .master("local[4]")
      .appName("Assignment Unit Tests")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession
  }

}