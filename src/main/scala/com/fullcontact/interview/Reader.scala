package com.fullcontact.interview

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Reader {
  def recordReader(path: String, sparkContext: SparkContext): RDD[Array[String]] = sparkContext.textFile(path).map(_.split(" "))
  def queriesReader(path: String, sparkContext: SparkContext): RDD[String] = sparkContext.textFile(path)
}
