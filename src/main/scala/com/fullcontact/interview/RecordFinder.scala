package com.fullcontact.interview

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list}

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
    val records = spark.read.text("Records.txt")

    //Tag each element of the record with the full record
    val tagged = records.flatMap(r => {
      val value = r.getString(0)
      val array = value.split(" ").toSeq
      array.map(ele => (ele, array))
    }).toDF("id", "associated").repartition(col("id")).cache()

    //Get the queries
    val queries = spark.read.text("Queries.txt").repartition(col("value"))

    //Run the query
    val found = queries.join(tagged, queries("value") === tagged("id"), "inner")
      .select("id", "associated")

    println(found.count()) //32314 matches

    //Take each match and return the values
    val output1 = found.map(r => {
      val id = r.getString(0)
      val ids = r.getAs[Seq[String]](1)

      s"$id: ${ids.mkString(" ")}"
    })

    output1.coalesce(1).write.mode("overwrite").text("Output1.txt")

    //Take each match, collect the id sets, reduce to unique values in a parallel manner
    //Alternative would be to do the the uniqueness pass with a UDAF
    val output2 = found.groupBy(col("id"))
      .agg(collect_list("associated").as("allAssociated"))
      .map(r => {
        val id = r.getString(0)
        val idSets = r.getAs[Seq[Seq[String]]](1)
        val ids = idSets.map(_.toSet).reduce(_.union(_)) //Union to remove uniques

        s"$id: ${ids.mkString(" ")}"
      })

    output2.coalesce(1).write.mode("overwrite").text("Output2.txt")
  }
}
