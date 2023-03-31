package com.fullcontact.interview

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list}

case class Record(id: String, associated: Seq[String])

/**
 * Solution pattern
 */
object RecordFinder {
  def main(args: Array[String]): Unit = {
    println("main")
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Identity Resolution")
      .getOrCreate()

    import spark.implicits._

    //Read the records
    val records = spark.read.text("Records.txt").as[String]

    //Get the queries
    val queries = spark.read.text("Queries.txt").repartition(col("value")).as[String]

    //Tag each element of the record with the full record
    val allRecords = parseRecords(records).repartition(col("id")).cache()

    //Run the query
    val qr1 = runQuery(allRecords, queries)
    makeForOutput(qr1).coalesce(1).write.mode("overwrite").text("Output1.txt")

    //Run the query and aggregate by id
    val qr2 = runQueryAgg(allRecords, queries)
    makeForOutput(qr2).coalesce(1).write.mode("overwrite").text("Output2.txt")
  }

  /**
   * Take a string of space separated records and construct a Record for each element with the whole set as associated
   * @param df the set of string inputs
   * @return a set of Records
   */
  def parseRecords(df: Dataset[String]): Dataset[Record] = {
    import df.sparkSession.implicits._

    df.flatMap(str => {
      val array = str.split(" ").toSeq
      array.map(ele => Record(ele, array))
    })
  }

  /**
   * Run the query against all the records and return every matching record for each query
   * @param records The set of records
   * @param queries The set of queries
   * @return the query results
   */
  def runQuery(records: Dataset[Record], queries: Dataset[String]): Dataset[Record] = {
    import records.sparkSession.implicits._

    records.joinWith(queries, records("id") === queries("value"), "inner").map(_._1)
  }

  /**
   * Run the query against all the records and return a list of every unique matching record value for each query
   *
   * This is implemented in a phased manner where the matches are collected and the a uniqueness pass is applied.
   * An alternative solution would be to due the uniqueness during aggregation with a UDAF.
   * Also to note this does not make use of caching if the runQuery result is also needed.
   * @param records The set of records
   * @param queries The set of queries
   * @return the query results
   */
  def runQueryAgg(records: Dataset[Record], queries: Dataset[String]): Dataset[Record] = {
    import records.sparkSession.implicits._

    runQuery(records, queries)
      .groupBy(col("id"))
      .agg(collect_list("associated").as("associated"))
      .map(r => {
        val id = r.getString(0)
        val idSets = r.getAs[Seq[Seq[String]]](1)
        val ids = idSets.map(_.toSet).reduce(_.union(_)) //Union to remove uniques

        Record(id, ids.toSeq)
      })
  }

  /**
   * Prepare the QueryResults for output
   * @param df the query results
   * @return a string of format: `(id): (id-match-1) ... (id-match-n)`
   */
  def makeForOutput(df: Dataset[Record]): Dataset[String] = {
    import df.sparkSession.implicits._

    df.map(r => {
      s"${r.id}: ${r.associated.mkString(" ")}"
    })
  }
}
