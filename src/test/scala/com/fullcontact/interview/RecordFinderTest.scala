package com.fullcontact.interview

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RecordFinderTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  //All lazy vals to delay creation until tests run
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Identity Resolution")
    .getOrCreate()

  lazy val recordSrcDf : Dataset[String] = {
    import spark.implicits._
    Seq(
      "a b c",
      "d e",
      "a z"
    ).toDS()
  }

  lazy val queryDf: Dataset[String] = {
    import spark.implicits._
    Seq(
      "a",
      "d",
      "z"
    ).toDS()
  }

  lazy val recordsDf: Dataset[Record] = {
    import spark.implicits._
    Seq(
      Record("a", Seq("a", "b", "c")), Record("b", Seq("a", "b", "c")), Record("c", Seq("a", "b", "c")),
      Record("d", Seq("d", "e")), Record("e", Seq("d", "e")),
      Record("a", Seq("a", "z")), Record("z", Seq("a", "z")),
    ).toDS()
  }

  lazy val queryRecords1Df: Dataset[Record] = {
    import spark.implicits._
    Seq(
      Record("a", Seq("a", "b", "c")),
      Record("d", Seq("d", "e")),
      Record("a", Seq("a", "z")),
      Record("z", Seq("a", "z"))
    ).toDS()
  }

  lazy val queryRecords2Df: Dataset[Record] = {
    import spark.implicits._
    Seq(
      Record("a", Seq("a", "b", "c", "z")),
      Record("d", Seq("d", "e")),
      Record("z", Seq("a", "z"))
    ).toDS()
  }

  lazy val queryOutput2 : Dataset[String] = {
    import spark.implicits._
    Seq(
      "a: a b c z",
      "d: d e",
      "z: a z"
    ).toDS()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  "parseRecords" should "convert id groups into relationships of each id ot the whole group" in {
    val res = RecordFinder.parseRecords(recordSrcDf)
    res.collect() should contain theSameElementsAs recordsDf.collect()
  }

  "runQuery" should "query the records and pull the relevant records" in {
    val res = RecordFinder.runQuery(recordsDf, queryDf)
    res.collect() should contain theSameElementsAs queryRecords1Df.collect()
  }

  "runQueryAgg" should "query the records and pull the relevant records" in {
    val res = RecordFinder.runQueryAgg(recordsDf, queryDf)
    res.collect() should contain theSameElementsAs queryRecords2Df.collect()
  }

  "makeForOutput" should "prepare the records for output by formatting them" in {
    val res = RecordFinder.makeForOutput(queryRecords2Df)
    res.collect() should contain theSameElementsAs queryOutput2.collect()
  }
}
