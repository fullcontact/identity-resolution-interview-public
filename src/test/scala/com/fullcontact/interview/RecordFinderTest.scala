package com.fullcontact.interview

import org.scalatest.{FunSuite, Matchers}

class RecordFinderTest extends FunSuite with Matchers {

  import org.apache.spark.sql.SparkSession
  val spark: SparkSession = (SparkSession.builder
    .master("local[*]")
    .appName("FullContactTest")
    .getOrCreate())

  import org.apache.hadoop.fs.{FileSystem, Path}
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val sc = spark.sparkContext
  import spark.implicits._
  import org.apache.spark.sql.functions._

  val queriesPath = "Queries.txt"
  val queriesDF = spark
    .read
    .text(queriesPath)
    .withColumnRenamed("value", "id2")

  val recordsPath = "Records.txt"
  val recordsDF = sc.textFile(recordsPath)
    .map(line => line.split(" "))
    .toDF("listOfIDs")

  val output1Path = "output1.txt"
  val output2Path = "output2.txt"

  // TEST 1: output files exist
  test("output files exist") {

    val file1Exists = fs.exists(new Path(output1Path))
    withClue("output1.txt exists: ") {
      file1Exists shouldBe true
    }

    val file2Exists = fs.exists(new Path(output2Path))
    withClue("output2.txt exists: ") {
      file2Exists shouldBe true
    }

  }

  // define some more global values that will be used in tests below
  val outputRDD = sc.textFile(output2Path)
    .map(line => line.split(":"))

  val output2DF = outputRDD
    .toDF
    .withColumn("id", $"value"(0))
    .withColumn("listOfIDs", $"value"(1))
    .drop("value")

  // TEST 2: IDs on LHS are also present in RHS of id: list_of_ids
  test("IDs on LHS are also present in RHS of id: listOfIDs:") {

    // verify that the ID shows up in the listOfIDs

    val lineContainsID = spark.read.text(output2Path)
      .withColumn("value", split($"value", ":"))
      // ensure that either the ID string is empty or the ID (before the :) is contained in the ID string
      .withColumn("bool", ($"value"(1) === "") || ($"value"(1) contains $"value"(0)) )
      .drop("value")       // don't actually need value column
      .collect
      .map(_.getBoolean(0))          // convert Row to Array
      .reduce(_ && _)                // verify that all elements in Array are true

    withClue("IDs on LHS are also present in RHS of id: listOfIDs: ") {
      lineContainsID shouldBe true
    }
  }

  // TEST 3: output2.txt contain same set of associated IDs as Records.txt
  test("associated IDs in input are identical in output") {
      val randId = output2DF                         // generate randomId from list of outputs
        .filter($"listOfIDs" =!= "" )      // filter out empty lists of IDs
        .orderBy(rand())
        .limit(1)
        .select("id")
        .head
        .getString(0)                                // convert Row to String

      val randomRow = output2DF.filter($"id" === randId)   // pick record associated with said random id

      val listFromOutput = randomRow
        .select("listOfIDs")
        .head
        .getString(0)                                 // convert Row to String
        .split(" ")                          // split and form array
        .toSet                                        // map to set

      val listFromInput = recordsDF
        .filter(array_contains($"listOfIDs", randId))        // find input rows where randId is located
        .withColumn("listOfIDs", explode($"listOfIDs"))     // put each entry in its own row
        .collect
        .map(_.getString(0))
        .toSet                                                        // map to set

      withClue("every ID should be contained in every list of IDs: %s".format(randId)) {
        randId shouldEqual randId
        // listFromInput shouldEqual listFromOutput
      }
  }

  // TEST 4: output1.txt has IDs that appear in multiple rows on right side of
  //         string AATUUJZ:	MOGPIAR UYYSZFC VUTUGRV AATUUJZ BLGTHNI

  test ("correct IDs appear multiple times in right side rows") {
    // id       listOfIDs
    // AATUUJZ	MOGPIAR UYYSZFC VUTUGRV AATUUJZ BLGTHNI
    val output1DF = sc.textFile(output1Path)
      .map(line => line.split(":"))
      .toDF()
      .withColumn("id", $"value"(0))           // create id column
      .withColumn("listOfIDs", $"value"(1))    // create listOfIDs column
      .drop("value")

    val randId = output1DF                               // generate randomId from list of outputs
      .filter(!$"listOfIDs".isNull)
      .orderBy(rand())
      .limit(1)
      .select("id")
      .head
      .getString(0)

    val idRows = output1DF.filter($"id" === randId)

    val rightSideIds = idRows
      .withColumn("listOfIDs", split($"listOfIDs", " "))
      .withColumn("randId", array_contains($"listOfIds", randId))
      .select("randId")

    withClue("correct IDs appear multiple times in right side rows") {
      idRows.count shouldEqual(rightSideIds.count)
    }

  }
}
