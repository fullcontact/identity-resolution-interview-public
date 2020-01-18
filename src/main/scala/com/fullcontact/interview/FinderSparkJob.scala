package com.fullcontact.interview

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class FinderSparkJob(sparkSession: SparkSession) {
  val log = Logger.getLogger(this.getClass)

  /**
   * Calls to do the work takes the resulting RDDs back to then write them
   * @param queryPath
   * @param recordPath
   * @param outputLocation1
   * @param outputLocation2
   * @param numFilePartitions
   */
  def genOutput(queryPath: String,
                 recordPath: String,
                 outputLocation1: String,
                 outputLocation2: String,
                 numFilePartitions: Int): Unit = {

    val (output1Rdd, output2Rdd) = getOutputRdds(queryPath,recordPath)

    output1Rdd
      .map{ case (k,v) => s"$k:${v.mkString(" ")}"}
      .coalesce(numFilePartitions)
      .saveAsTextFile(outputLocation1)

    output2Rdd
      .map{ case (k,v) => s"$k:${v.mkString(" ")}"}
      .coalesce(numFilePartitions)
      .saveAsTextFile(outputLocation2)
  }

  /**
   * Gets the output RDDs from the output 1 and 2 algorithms
   * @param queryPath
   * @param recordPath
   * @return
   */
  def getOutputRdds(queryPath: String,
                    recordPath: String): (RDD[(String,Array[String])],RDD[(String,Array[String])]) = {
    val sparkContext = sparkSession.sparkContext
    val queriesRdd: RDD[String] = sparkContext.textFile(queryPath)
    val recordsRdd: RDD[String] = sparkContext.textFile(recordPath)

    //Take the records and turn it into an array and validate each id
    val identifierLine: RDD[(String, Array[String])] = recordsRdd.flatMap{ line =>
      val identifierArray = line
        .split(" ")

      //Remove any ids that don't match our expected criteria
      val filteredIds = Config.VALIDATION_ON match {
        case true =>
          identifierArray.filter{ id =>
            id.size == Config.IDENTIFIER_SIZE &&
              id.filter(char => char.isUpper).size == Config.IDENTIFIER_SIZE
          }
        case _ =>
          identifierArray
      }

      filteredIds.map{id => (id.trim, filteredIds)}
    }

    //Output1 results
    val joinResult: RDD[(String, Array[String])] =
      queriesRdd
        .filter(id => id.size == Config.IDENTIFIER_SIZE &&
          id.filter(char => char.isUpper).size == Config.IDENTIFIER_SIZE)
      .map(key => (key.trim, Array.empty[String])) //Make this a KV so we can perform PairRDD functions
      .join(identifierLine)
      .mapValues{case (k,v) => k ++ v}

    //Output2 results
    val dedupedResult = joinResult
      .reduceByKey{ case (x,y) => (x ++ y).distinct}
      .mapValues(_.distinct) //if there is only 1 key the above reduce function with its distinct isn't applied

    (joinResult, dedupedResult)
  }
}
