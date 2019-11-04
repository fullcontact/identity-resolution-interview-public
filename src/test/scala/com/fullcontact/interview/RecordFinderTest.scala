package com.fullcontact.interview

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

import com.fullcontact.interview.utils.TestUtils._

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", false)
      .config("spark.driver.host", "localhost")
      .appName("Test Record finder").getOrCreate()
  }
}

class RecordFinderTest extends FunSuite with Matchers with SparkSessionTestWrapper {
  import spark.implicits._
  val recordFinder = new RecordFinder(spark)

  val recordRowsDS =  List(RecordRow(1L, "ABC DEF"),
    RecordRow(2L, "GHI DEF ABC"),
    RecordRow(3L, "SDF JKL")).toDS

  val explodedRecordRowsDS = List(RecordRowExploded(1L, "ABC"),
    RecordRowExploded(1L, "DEF"),
    RecordRowExploded(2L, "GHI"),
    RecordRowExploded(2L, "DEF"),
    RecordRowExploded(2L, "ABC"),
    RecordRowExploded(3L, "SDF"),
    RecordRowExploded(3L, "JKL")).toDS

  val queryRecordsDS = List(QueryRow("ABC"), QueryRow("DEF"), QueryRow("SDF"), QueryRow("IIJ")).toDS

  val report1DS = List(IdentifierWithRow("ABC", Some("GHI DEF ABC")),
    IdentifierWithRow("ABC", Some("ABC DEF")),
    IdentifierWithRow("DEF", Some("GHI DEF ABC")),
    IdentifierWithRow("DEF", Some("ABC DEF")),
    IdentifierWithRow("SDF", Some("SDF JKL")),
    IdentifierWithRow("IIJ", None)).toDS

  val report2DS = List(IdentifierWithRelatedIds("ABC", Some("GHI DEF ABC")),
    IdentifierWithRelatedIds("DEF", Some("GHI DEF ABC")),
    IdentifierWithRelatedIds("SDF", Some("SDF JKL")),
    IdentifierWithRelatedIds("IIJ", Some(""))).toDS

  test("Test explodeRecords") {
    val actualOutputDS = recordFinder.explodeRecords(recordRowsDS)
    unorderedCompareDatasets(actualOutputDS, explodedRecordRowsDS, Seq("rowId"))
  }

  test("Test Generate report 1") {
    val actualOutputDS = recordFinder.generateReport1(queryRecordsDS,
      explodedRecordRowsDS, recordRowsDS)
    unorderedCompareDatasets(actualOutputDS, report1DS, Seq("identifier"))
  }

  // TODO: Can make test more resilient to ordering of identifiers
  test("Test Generate report 2") {
    val actualOutputDS = recordFinder.generateReport2(report1DS)
    unorderedCompareDatasets(actualOutputDS, report2DS, Seq("identifier"))
  }
}
