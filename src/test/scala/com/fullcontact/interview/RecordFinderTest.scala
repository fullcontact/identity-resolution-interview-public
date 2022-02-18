package com.fullcontact.interview

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class RecordFinderTest extends FunSuite with Matchers with BeforeAndAfterEach {
  var sparkContext : SparkContext = _
  val records = Seq("a b c d",
    "c d e",
    "f g h")
  val queries = Seq("a", "d", "g")

  override def beforeEach() { // not really needed for all tests below.
    sparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("testing"))
  }

  override def afterEach() {
    sparkContext.stop()
  }

  test("record creation from string in correct order") {
    val record = Record("a b c")
    assert(record.ids == Seq("a", "b", "c")) // ensure order is preserved
  }

  test("record creation from string captures all IDs") {
    val record = Record("a b c")
    assert(record.ids.contains("a"))
    assert(record.ids.contains("b"))
    assert(record.ids.contains("c"))
  }

  test("record converted to string correctly") {
    val record = Record("a b c")
    assert(record.toString == "a b c")
  }

  test("record merging with unique ids") {
    val record1 = Record("a b c")
    val record2 = Record("c d a")
    val mergedRecord = Record.getMergedRecord(Seq(record1, record2))

    // when merged it should contain a b c d
    assert(mergedRecord.ids.size == 4)
    assert(mergedRecord.ids.sorted == Seq("a", "b", "c", "d"))
  }

  test("Output1: query to record mapping") {
    val recordsRdd = sparkContext.parallelize(records)
    val queriesRdd = sparkContext.parallelize(queries)
    val (output1, _) = RecordFinder.getQueryToRecordRdd(queriesRdd, recordsRdd)
    val results = output1.collect()
    assert(results.size == queries.size + 1) // ID "d" has two records
    assert(results.contains("a:a b c d"))
    assert(results.contains("d:a b c d"))
    assert(results.contains("d:c d e"))
    assert(results.contains("g:f g h"))
  }

  test("Output2: query to record mapping with unique ids") {
    val recordsRdd = sparkContext.parallelize(records)
    val queriesRdd = sparkContext.parallelize(queries)
    val (_, output2) = RecordFinder.getQueryToRecordRdd(queriesRdd, recordsRdd)
    val results = output2.collect()
    assert(results.size == queries.size)
    val validMergedRecords = Map("a" -> Record("a b c d"), "d" -> Record("a b c d e"), "g" -> Record("f g h"))
    for (resultRecord <- results) {
      val (query, record) = parseQueryRecord(resultRecord)
      assert(validMergedRecords.contains(query))
      assert(validMergedRecords(query).ids.sorted == record.ids.sorted)
    }
  }

  test("when no record exists for query") {
    val recordsRdd = sparkContext.parallelize(records)
    val queriesRdd = sparkContext.parallelize(Seq("NotAvailableID"))
    val (output1, output2) = RecordFinder.getQueryToRecordRdd(queriesRdd, recordsRdd)
    val results1 = output1.collect()
    assert(results1.size == 1)
    assert(results1.toSeq == Seq("NotAvailableID:"))
    val results2 = output2.collect()
    assert(results2.size == 1)
    assert(results2.toSeq == Seq("NotAvailableID:"))
  }

  private def parseQueryRecord(input: String):(String, Record) = {
    val strSplit = input.split(":")
    (strSplit(0), Record(strSplit(1)))
  }

}
