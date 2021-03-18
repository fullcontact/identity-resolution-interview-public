package com.fullcontact.interview

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
object RecordFinder {


  def getIdentityMatch(spark: SparkSession,records: Dataset[String], queries: Dataset[String]): Dataset[String] = {

    import spark.implicits._

    // Udf function to flatten the arrays during aggeration and remove duplicates
    val flat_dedupe = udf(
      (x: Seq[Seq[String]]) => x.flatten.distinct)

    // Formats the output in the required format
    val output_format = (row:Row) => { row.getAs[String](0) + ":" + row.getAs[Seq[String]](1).mkString(" ")}


    val keyValue_records=records.flatMap { line =>
      line.split(" ").
        map(word => (word, line.split(" ")))
    }.toDF("key", "values")

    val query_df=queries.toDF("queries_key")
    val matched_records = query_df.
      join(keyValue_records, query_df("queries_key") === keyValue_records("key"), "left").
      select("queries_key", "values").filter("values is not null" )

    matched_records.map(output_format)
      .write.text("Output1.txt")

    val output = matched_records.groupBy("queries_key")
      .agg(flat_dedupe(collect_set(matched_records("values"))))
      .map(output_format)
    output
  }


  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "C:\\Users\\Ashwin.Kumar\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")

    val spark = SparkSession.builder.
      master("local")
      .appName("Identity Resolution")
      .getOrCreate()


    val records = spark.read.textFile("Records.txt")

    val queries = spark.read.textFile("Queries.txt")

    val output = getIdentityMatch(spark,records, queries)
    output.show(false)
    output.coalesce(1).write.text("Output2.txt")
  }


}
