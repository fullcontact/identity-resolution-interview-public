package com.fullcontact.interview

import org.apache.spark.rdd.RDD

object Writer {

  def writeToFile(rdd: RDD[(String, Array[String])], outputPath: String): Unit = {
    rdd.map{case(key, value) => key + value.mkString("", " ", "")}.coalesce(1).saveAsTextFile(outputPath)
  }
}
