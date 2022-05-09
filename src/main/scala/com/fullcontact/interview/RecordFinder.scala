package com.fullcontact.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col,collect_list, array_distinct, flatten}

/**
 * Main Class to build and run the spark job
 */
case class RecordFinder() {

  /**
   * Main method to start the spark job
   * @param spark initialized spark session
   */
  def start(implicit spark: SparkSession): Unit = {

    //declare encoders needed for serializing data structures
    implicit val stringEncorder: Encoder[String] = Encoders.STRING
    implicit val recordsEncoder: Encoder[List[String]] = Encoders.product[List[String]]
    implicit val resultsEncoder: Encoder[(String, List[String])] = Encoders.product[(String, List[String])]

    //read in data
    val recordsDs = spark.read.text("Records.txt").withColumnRenamed("value", "idList")

    //read in queries
    val queriesDf = spark.read.text("Queries.txt").withColumnRenamed("value", "queryId")

    //inner join using contains operator and cache to speed up writing to both files
    //note that string contains() might not be the most efficient operator here but works.
    val resultsDf = queriesDf.join(recordsDs).where( col("idList").contains(col("queryId"))).cache()

    //map to a dataset to make it easier to process as a tuple with the following structure (queryId, resultList)
    val resultsDs = resultsDf.map(r=>(r.getString(r.fieldIndex("queryId")),
                                      r.getString(r.fieldIndex("idList")).split(" ").toList))

    //group the list of lists per query ID and flatten them.
    val groupedDs = resultsDs
       .groupByKey { case(queryId, _) => queryId }
       .mapGroups( (queryId,results)=> (queryId, results.toList.flatMap{case (_, resultList) => resultList.distinct}))

    //output first file
    resultsDs
      .map{case(queryId, resultList) => s"$queryId: ${resultList.mkString(" ")}"}
      .write
      .mode("overwrite")
      .text("Output1.txt")

    //output second file
    groupedDs
      .map{case(queryId, resultList) => s"$queryId: ${resultList.mkString(" ")}"}
      .write
      .mode("overwrite")
      .text("Output2.txt")
  }
}

/**
 * Companion Object used to initialize Spark Session and Run
 */
object RecordFinder {

  /**
   * Main driver
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val config = new SparkConf()
     config.set("spark.debug.maxToStringFields", "10000")


    // create the SparkSession
    val builder = SparkSession.builder()
      .appName("RecordFinder")
      .config(config)

    // use only local mode
    implicit val sparkSession = builder.master("local[*]").getOrCreate()

    val recordFinder = new RecordFinder
    recordFinder.start
  }
}
