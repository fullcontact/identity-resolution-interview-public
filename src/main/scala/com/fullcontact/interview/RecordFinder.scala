package com.fullcontact.interview

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD;

object RecordFinder {
  def pipleLine1(records: RDD[(String, Set[String])], queries: RDD[(String, String)]): RDD[String] = {
    val keyedRecordsInQueries: RDD[(String, (Set[String], String))] = records.join(queries)
//    keyedRecordsInQueries.take(5).foreach(println)
    keyedRecordsInQueries.map { rec =>
      s"""${rec._1}: ${rec._2._1.mkString(" ")}"""
    }
  }

  def pipleLine2(records: RDD[(String, Set[String])], queries: RDD[(String, String)]): RDD[String] = {
    val reducedKeyedRecords: RDD[(String, Set[String])] = records.reduceByKey { (A, B) =>
      A ++ B
    }

    val reducedKeyedRecordsInQueries: RDD[(String, (Set[String], String))] = reducedKeyedRecords.join(queries)
//    reducedKeyedRecordsInQueries.take(5).foreach(println)
    reducedKeyedRecordsInQueries.map { rec =>
      s"""${rec._1}: ${rec._2._1.mkString(" ")}"""
    }
  }

  // Convert each record to a map
  // eg.
  //
  // Convert [A, B, C]
  //
  // to
  //
  // (A -> (A, B, C)
  // (B -> (A, B, C)
  // (C -> (A, B, C)
  def makeRecordMap(records: RDD[String]): RDD[(String, Set[String])] = {
    records.flatMap { rec =>
      val recs: Array[String] = rec.split(' ')

      recs.map { r => (r, recs.toSet)
      }
    }
  }

  def main(args: Array[String]) {
    val recordsPath = "Records.txt"
    val queriesPath = "Queries.txt"
    val output1Path = "Output1"
    val output2Path = "Output2"
    println(s"recordsPath: ${recordsPath}, queriesPath: ${queriesPath}, output1Path: ${output1Path}, output2Path: ${output2Path}")

    // initialise spjark context
    val conf = new SparkConf().setAppName(RecordFinder.getClass.getName)
    conf.setMaster("local")
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    // Read the records
    val records: RDD[String] = spark.sparkContext.textFile(recordsPath)
    println(s"Records Count: ${records.count}")

    // Read the queries
    val queries: RDD[String] = spark.sparkContext.textFile(queriesPath)
    println(s"Queries Count: ${queries.count}")

    val keyedRecords: RDD[(String, Set[String])] = makeRecordMap(records)
    val keyedQueries: RDD[(String, String)] = queries.map(q => (q, q))

    val output1 = pipleLine1(keyedRecords, keyedQueries)
    output1.saveAsTextFile(output1Path)

    val output2 = pipleLine2(keyedRecords, keyedQueries)
    output2.saveAsTextFile(output2Path)

    println("*****All Done!*******")
    spark.stop()
  }
}

