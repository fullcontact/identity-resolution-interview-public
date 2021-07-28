package com.fullcontact.interview.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkInit {

  def createSession(): SparkSession

  protected def minimalConf: SparkConf = {
    val configuration = new SparkConf()
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24m")
      .set("spark.scheduler.mode", "FAIR")
    configuration
  }
}

class DefaultSparkInit extends SparkInit {
  override def createSession(): SparkSession = {
    SparkSession.builder().config(minimalConf).appName("Assignment for finding query id")
      .getOrCreate()
  }
}
