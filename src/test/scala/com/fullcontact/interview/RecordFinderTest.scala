package com.fullcontact.interview

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

class RecordFinderTest extends FunSuite with Matchers {

  val conf = new SparkConf()
    .setAppName("BradsRecordFinderTest")
    .setMaster("local[4]")
  val sc = new SparkContext(conf)

  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
  import spark.implicits._

  val output1DFsortChk = Seq(
    ("AAAAAAA", Array("AAAAAAA", "BBBBBBB")),
    ("AAAAAAA", Array("AAAAAAA", "BBBBBBB", "CCCCCCC")),
    ("BBBBBBB", Array("AAAAAAA", "BBBBBBB")),
    ("BBBBBBB", Array("AAAAAAA", "BBBBBBB", "CCCCCCC")),
    ("DDDDDDD", Array("DDDDDDD")),
    ("FFFFFFF", Array("FFFFFFF", "EEEEEEE", "GGGGGGG", "HHHHHHH")),
  ).toDF("ID", "partialNeighborArray")

  test("Proper validation of an ID having/not having 7 uppercase letters") {
    assert(RecordFinder.isWordNot7Uppers("ABCDEFG") == 0)
    assert(RecordFinder.isWordNot7Uppers("ABCDEFg") == 1)
    assert(RecordFinder.isWordNot7Uppers("ABCD FG") == 1)
    assert(RecordFinder.isWordNot7Uppers("1BCDEFG") == 1)
  }

  test("Proper validation of # non-7-uppercase-letter words in array") {
    val goodArr = Array("ABCDEFG", "BCDEFGH", "ZYXWVUT")
    val badArr1 = Array("ABCDEFG", "BCDEFGH", "ZYXWVU1")
    val badArr2 = Array("ABCDEFG", "BCDEFG", "ZYXWVUT")
    val badArr3 = Array("aBCDEFG", "BCDEFGH", "ZYXWVUT")
    val badArr4 = Array("aBCDEFG", "BCDEFG1", "ZYXWVUT", "")

    assert(RecordFinder.areWordsNot7Uppers(goodArr).sum == 0)
    assert(RecordFinder.areWordsNot7Uppers(badArr1).sum == 1)
    assert(RecordFinder.areWordsNot7Uppers(badArr2).sum == 1)
    assert(RecordFinder.areWordsNot7Uppers(badArr3).sum == 1)
    assert(RecordFinder.areWordsNot7Uppers(badArr4).sum == 3)
  }

  test("Validation of Queries input after RDD transformation") {
    val goodQRdd = sc.parallelize(Array("ABCDEFG","HIJKLMN","OPQRSTU"))
    val badQRdd1 = sc.parallelize(Array("ABCDEFG","HIJKLMN","OPQRST1"))
    val badQRdd2 = sc.parallelize(Array("ABCDEFG","HIJKLMn","OPQRSTU"))
    val badQRdd3 = sc.parallelize(Array("ABCDEF","HIJKLMn","OPQRSTU"))
    val sa : Array[String] = Array()
    val badQRdd4 = sc.parallelize(sa)

    assert(RecordFinder.validateQueries(goodQRdd))
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd1)}
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd2)}
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd3)}
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd4)}
  }

  test("Validation of Records after RDD transform") {
    val goodArr1 = Array("ABCDEFG","HIJKLMN","OPQRSTU")
    val goodArr2 = Array("ABCDEFG","HIJKLMN","OPQRSTU", "AAAAAAA")
    val goodArr3 = Array("ABCDEFG")
    val badArr1 = Array("ABCDEFG","HIJKLMN","OPQRST1")
    val badArr2 = Array("ABCDEFG","HIJKLMn","OPQRSTU")
    val badArr3 = Array("ABCDEF","HIJKLMn","OPQRSTU")

    val goodRRdd1 = sc.parallelize(Array(goodArr1, goodArr2, goodArr3))
    val goodRRdd2 = sc.parallelize(Array(goodArr1, goodArr2, goodArr3))
    val badRRdd1 = sc.parallelize(Array(goodArr1, goodArr2, badArr1))
    val badRRdd2 = sc.parallelize(Array(goodArr1, goodArr2, badArr2))
    val badRRdd3 = sc.parallelize(Array(goodArr1, badArr3, goodArr3))

    assert(RecordFinder.validateRecords(goodRRdd1))
    assert(RecordFinder.validateRecords(goodRRdd2))
    assertThrows[RuntimeException]{RecordFinder.validateRecords(badRRdd1)}
    assertThrows[RuntimeException]{RecordFinder.validateRecords(badRRdd2)}
    assertThrows[RuntimeException]{RecordFinder.validateRecords(badRRdd3)}
  }

  test("Generating expected Output1 DataFrame from Records and Queries") {
    val recordsSplitRDD = sc.textFile("./RecordsTest.txt")
      .map(l => l.split(" "))
    val queriesRDD = sc.textFile("./QueriesTest.txt")

    val testOutput1DFsorted = RecordFinder.generateOutput1DF(recordsSplitRDD, queriesRDD)
      .orderBy("ID", "partialNeighborArray")

    assert(testOutput1DFsorted.schema.equals(output1DFsortChk.schema))
    assert(testOutput1DFsorted.collect().sameElements(output1DFsortChk.collect()))
  }

  test("Generating expected Output1 Final RDD") {
    val testRdd = RecordFinder.generateFinalOutput1RDD(output1DFsortChk)
    val testFinalOut1DFsorted = testRdd.toDF("solutionRow")
      .orderBy("solutionRow")

    val out1FinalDFchkSorted = Seq(
      ("AAAAAAA: AAAAAAA BBBBBBB"),
      ("DDDDDDD: DDDDDDD"),
      ("FFFFFFF: FFFFFFF EEEEEEE GGGGGGG HHHHHHH"),
      ("BBBBBBB: AAAAAAA BBBBBBB CCCCCCC"),
      ("BBBBBBB: AAAAAAA BBBBBBB"),
      ("AAAAAAA: AAAAAAA BBBBBBB CCCCCCC")
    ).toDF("solutionRow")
      .orderBy("solutionRow")

    assert(testFinalOut1DFsorted.collect().sameElements(out1FinalDFchkSorted.collect()))
  }

  test("Generating expected Output2 Final RDD") {
    val testRdd = RecordFinder.generateFinalOutput2RDD(output1DFsortChk)
    val testFinalOut2DFsorted = testRdd.toDF("solutionRow")
      .orderBy("solutionRow")

    val out2FinalDFchkSorted = Seq(
      ("AAAAAAA: AAAAAAA BBBBBBB CCCCCCC"),
      ("BBBBBBB: AAAAAAA BBBBBBB CCCCCCC"),
      ("DDDDDDD: DDDDDDD"),
      ("FFFFFFF: FFFFFFF EEEEEEE GGGGGGG HHHHHHH")
    ).toDF("solutionRow")
      .orderBy("solutionRow")

    assert(testFinalOut2DFsorted.collect().sameElements(out2FinalDFchkSorted.collect()))
  }
}
