package com.fullcontact.interview

import com.fullcontact.interview.RecordFinder.mergeArraysBySearchID
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, split, trim}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FunSuite, Matchers}

class RecordFinderTest extends FunSuite with Matchers with DataFrameSuiteBase {

  test("unmergedRawDF, Happy Path, Very Light") {
    val recordsDF: DataFrame = createRecordsDF()
    val searchIdsDF: DataFrame = createSearchIdsDF()

    val unmergedResult: DataFrame = RecordFinder.searchUnMergedRecords(searchIdsDF, recordsDF)
    val unmergedResultList: List[Row] = unmergedResult.collect().toList
    assertResult(6)(unmergedResultList.size)

    val kIdNumber = getNumberOfRecordsByKey(unmergedResultList, "KDYBNMV")
    assertResult(3)(kIdNumber)
    val wIdNumber = getNumberOfRecordsByKey(unmergedResultList, "WFNXANO")
    assertResult(2)(wIdNumber)
    val sIdNumber = getNumberOfRecordsByKey(unmergedResultList, "SXFVGJS")
    assertResult(1)(sIdNumber)

    val unmergedFormattedResult: DataFrame = RecordFinder.formatOutPutDF(unmergedResult)
    val unmergedFormattedResultList: List[Row] = unmergedFormattedResult.collect().toList
    assertResult(6)(unmergedFormattedResultList.size)

    val unmergedFormattedResultSet: Set[Row] = unmergedFormattedResultList.toSet

    val expectedUnmergedFormattedResultSet = Set(Row("KDYBNMV:KDYBNMV ABCDEFG HIJKLMN"),
      Row("KDYBNMV:KDYBNMV HIJKLMN OPQRSTU"),
      Row("KDYBNMV:KDYBNMV VWXZYAB ABCDEFG"),
      Row("WFNXANO:WFNXANO CDEFGHI JKLMONP"),
      Row("WFNXANO:WFNXANO JKLMONP QRSTUVW XYZABCD"),
      Row("SXFVGJS:SXFVGJS EFGHIJK LMNOPQR STUVWXY ZABCDEF"))

    assertResult(expectedUnmergedFormattedResultSet)(unmergedFormattedResultSet)


    val mergedResult: DataFrame = RecordFinder.mergeArraysBySearchID(unmergedResult)
    val mergedFormattedResult: DataFrame = RecordFinder.formatOutPutDF(mergedResult)
    val mergedFormattedResultList: List[Row] = mergedFormattedResult.collect().toList
    val mergedFormattedResultSet: Set[Row] = mergedFormattedResultList.toSet

    val expectedMergedFormattedResultSet = Set(
      Row("KDYBNMV:KDYBNMV VWXZYAB ABCDEFG HIJKLMN OPQRSTU"),
      Row("WFNXANO:WFNXANO CDEFGHI JKLMONP QRSTUVW XYZABCD"),
      Row("SXFVGJS:SXFVGJS EFGHIJK LMNOPQR STUVWXY ZABCDEF"))

    assertResult(expectedMergedFormattedResultSet)(mergedFormattedResultSet)
  }

  def getNumberOfRecordsByKey(testList: List[Row], id: String): Int = {
    var found = 0
    testList.foreach {
      row: Row =>
        val testId = row(0)
        if (testId == id) {
          found += 1
        }
    }
    found
  }


  def createRecordsDF(): DataFrame = {
    import spark.implicits._
    val rdd: RDD[String] = spark.sparkContext.parallelize(testRecordStrings)
    var df: DataFrame = rdd.toDF("value")
    df = df.withColumn("trimmed", trim($"value"))
    df = df.select($"trimmed", split(col("value"), "\\s+").as("idarray"))
    df = df.drop("trimmed", "value")
    df.show(3)
    df
  }

  def createSearchIdsDF(): DataFrame = {
    import spark.implicits._
    val rdd: RDD[String] = spark.sparkContext.parallelize(testSearchIds)
    var df: DataFrame = rdd.toDF("value")
    df = df
      .withColumn("searchkey", trim($"value"))
      .drop("value")
      .dropDuplicates()
    df.show(3)
    df
  }


  val testSearchIds: Seq[String] = Seq("KDYBNMV", "WFNXANO", "SXFVGJS")
  val testRecordStrings: Seq[String] = Seq(
    "KDYBNMV ABCDEFG HIJKLMN",
    "KDYBNMV HIJKLMN OPQRSTU",
    "KDYBNMV VWXZYAB ABCDEFG",
    "WFNXANO CDEFGHI JKLMONP",
    "WFNXANO JKLMONP QRSTUVW XYZABCD",
    "SXFVGJS EFGHIJK LMNOPQR STUVWXY ZABCDEF",
    "YWRRPMR XPVBTQE PBFRVDD SQFHASF",
    "WNOSVHQ SSATKEJ KROZJRA GTXDPZL GACTECQ")
}
