package com.fullcontact.interview
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/***
  * The file process the records and query input and join if it matches any of the value in the records file irrespective of the match location
  */

object RecordFinder {
  def main(args: Array[String]): Unit = {

    //defininig schema for records file
    val schemaString = "id1 id2 id3 id4 id5"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable=true))
    val schemaRecords = StructType(fields)

    //defining schema for query file
    val schemaString = "id1"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable=true))
    val schemaQuery = StructType(fields)

    //read records file from input source
    val records = sqlContext.read.format("csv")
      .option("header", "false")
      .option("delimiter"," ")
      .schema(schemaRecords)
      .load("s3://midhun/Records.txt")

    //repartition based on all column since we have join on multiple columns
    val df_records = records.toDF().repartition(col("id1"), col("id2"), col("id3"), col("id4"), col("id5"))
    df_records.registerTempTable("records")

    //group by records table to remove duplicates
    val records_deduped =  sqlContext.sql("SELECT record.id1,record.id2,record.id3,record.id4,record.id5, MAX(1) from records as record GROUP BY record.id1,record.id2,record.id3,record.id4,record.id5")
    records_deduped.registerTempTable("records_deduped")

    //read records file from input source
    val queries = sqlContext.read.format("csv")
      .option("header", "false")
      .option("delimiter"," ")
      .schema(schemaQuery)
      .load("s3://midhun/Queries.txt")

    val df_queries = queries.toDF()

    df_queries.registerTempTable("queries")

    ////join query table with records table on all the columns since ids in queries can occur anywhere in records table, since the records table is deduped we won't have duplicates
    val result_df = sqlContext.sql("SELECT CONCAT(queries.id1, ':', record5.id1) AS id1,record5.id2,record5.id3,record5.id4,record5.id5 from queries INNER JOIN records_deduped record5 ON record5.id5 = queries.id1 OR record5.id4 = queries.id1 OR record5.id3 = queries.id1 OR record5.id2 = queries.id1 OR record5.id1 = queries.id1")
    //join query table with records table on all the columns since ids in queries can occur anywhere in records table
    val result_df = sqlContext.sql("SELECT CONCAT(queries.id1, ':', record5.id1) AS id1,record5.id2,record5.id3,record5.id4,record5.id5 from queries INNER JOIN records record5 ON record5.id5 = queries.id1 OR record5.id4 = queries.id1 OR record5.id3 = queries.id1 OR record5.id2 = queries.id1 OR record5.id1 = queries.id1")

    result_df.write.format("csv").mode("overwrite").option("sep"," ").option("path","s3://midhun/output1").saveAsTable("output1")
    result_df.write.format("csv").mode("overwrite").option("sep"," ").option("path","s3://midhun/output2").saveAsTable("output2")
  }
}
