package com.fullcontact.interview

object RecordFinder {
  def main(args: Array[String]): Unit = {

    // here is Spark boilerplate to be able to run from IDE
    import org.apache.spark.sql.SparkSession

    val sparkSession = (SparkSession.builder
      .master("local[*]")
      .appName("FullContact")
      .getOrCreate())

    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // make sure to use the number of cores I have to do joins.. default is 200.. don't have that many cores!
    sparkSession.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

    /* terminology:
      - id, id2 is one such "WEHXGJD"
      - text, text2 is a string like "CWRIMTO DSOSWAB MYBWZRH CGNZJCZ GZIJNTZ"
      - listOfIDs a List("CWRIMTO", "DSOSWAB", "MYBWZRH", "CGNZJCZ", "GZIJNTZ") */

    val recordsPath = "Records.txt"
    val queriesPath = "Queries.txt"

    // load Queries file
    val queriesDF = (sc.textFile(queriesPath))
      .toDF
      .withColumnRenamed("value", "id2")

    // load Records file
    val recordsDF = sc.textFile(recordsPath)
      .map(line => (line.split(" "), line))          // split text into separate words
      .toDF("listOfIDs", "text")                  // create DF with one column listOfIDs, other col, original text
      .withColumn("id", explode($"listOfIDs"))     // explode listOfIDs into separate rows for each id
      .drop("listOfIDs")                           // we don't really need the list of separate IDs (keep original text)
      .select("id", "text")

    // list of IDs that are present in Records.txt but not Queries.txt
    val orphansinRecords = (recordsDF.select("id") except queriesDF.select("id2")).collect.map(_.getString(0))

    // dataframe of IDs that are present in Queries.txt but not Records.txt
    val orphansinQueriesDF = (queriesDF.select("id2") except recordsDF.select("id")).withColumn("text", lit(""))

    // the intersection of queries and 1st column of recordsDF
    // drop orphaned ID records
    // append orphaned ID queries
    val joinedRecQueryDF = recordsDF
      .join(queriesDF, queriesDF("id2") === recordsDF("id"))
      .filter(!$"id".isin(orphansinRecords : _*))      // isin requires that we unpack the values from List(values)
      .drop("id2")
      .union(orphansinQueriesDF)                       // add orphaned Queries

    // write part 1 to file
    joinedRecQueryDF
      .map(r => r(0) + ":" + r(1))                           // print as "id: List(ids)"
      .coalesce(1)                              // form one output file
      .write
      .mode("overwrite")
      .text("output1.txt")

    // part 2
    val groupedIDsDF = joinedRecQueryDF
      .withColumn("listOfIDs", split($"text", " ")) // form array from list of IDs
      .groupBy("id")
      .agg(array_distinct(flatten(collect_list("listOfIDs"))) //  deduplicated set of the union of all IDs from rows
        .alias("listOfIDs"))
      .select("id", "listOfIDs")

    // write part 2 to file
    // Assuming that if there are NO records in Records.txt for a given ID in Query.txt, to leave it out?
    groupedIDsDF
      .withColumn("listOfIDs", concat_ws(" ", $"listOfIDs"))
      .map(r => r(0) + ":" + r(1))                            // print as "id: List(ids)"
      .coalesce(1)                               // form one output file
      .write
      .mode("overwrite")
      .text("output2.txt")

  }
}
