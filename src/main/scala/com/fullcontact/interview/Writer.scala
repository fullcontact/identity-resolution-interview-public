package com.fullcontact.interview

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File
import scala.reflect.io.Directory

object Writer {

  def writeToFile(rdd: RDD[(String, Array[String])], outputPath: String): Unit = {
    rdd.map{case(key, value) => key + value.mkString("", " ", "")}.coalesce(1).saveAsTextFile(outputPath)
  }
}
