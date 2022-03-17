package com.fullcontact.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder(app: String) {

  lazy val sparkConf : SparkConf = new SparkConf()
    .setAppName(app)
    .setMaster("local[*]")

  lazy val sparkSession : SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}