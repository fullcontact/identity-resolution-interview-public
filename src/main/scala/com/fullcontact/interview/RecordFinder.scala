package com.fullcontact.interview
import org.apache.spark.{SparkConf, SparkContext}

object RecordFinder {
  def main(args: Array[String]): Unit = {
    // Spark setup/init
    val conf = new SparkConf()
      .setAppName("BradsRecordFinder")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Reading info from Records.txt file
    val recordsRawRDD = sc.textFile("Records.txt")
    println("Records RDD sample:")
    recordsRawRDD.take(10).foreach(println)
  }
}
