package com.fullcontact.interview

import com.fullcontact.interview.Record.getMergedRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RecordFinder {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("query-records"))
    try {
      val recordsRdd = sc.textFile("Records.txt")
      val queriesRdd = sc.textFile("Queries.txt")

      val (output1Rdd, output2Rdd) = getQueryToRecordRdd(queriesRdd, recordsRdd)

      output1Rdd.saveAsTextFile("Output1.txt")
      output2Rdd.saveAsTextFile("Output2.txt")
    } catch {
      case ex: Exception =>
        println(s"Got exception while processing records: ${ex.getMessage}", ex)
    }
  }

  def getQueryToRecordRdd(queriesRdd: RDD[String], recordsRdd: RDD[String]): (RDD[String], RDD[String]) = {
    val recordsById = recordsRdd.flatMap { recordStr =>
      val record = Record(recordStr)
      record.ids.map { id =>
        (id, record)
      }
    }
    val queryToRecordMap = queriesRdd.map(query => (query, query)).leftOuterJoin(recordsById).map { case (query, (_, recordOpt)) =>
      (query, recordOpt)
    }
    val output1 = queryToRecordMap.map { case (query, recordOpt) =>
      s"$query:${recordOpt.map(_.toString).getOrElse("")}"
    }
    val output2 = queryToRecordMap.groupBy(_._1).map { case (query, groupedValues) =>
      val records = groupedValues.flatMap(_._2)
      val mergedRecord = getMergedRecord(records)
      s"$query:$mergedRecord"
    }
    (output1, output2)
  }
}
