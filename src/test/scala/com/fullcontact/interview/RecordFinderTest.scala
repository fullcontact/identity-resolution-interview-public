package com.fullcontact.interview

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

class RecordFinderTest extends FunSuite with Matchers {

  val conf = new SparkConf()
    .setAppName("BradsRecordFinderTest")
    .setMaster("local[4]")
  val sc = new SparkContext(conf)

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

    assert(RecordFinder.validateQueries(goodQRdd) == true)
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd1)}
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd2)}
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd3)}
    assertThrows[RuntimeException]{RecordFinder.validateQueries(badQRdd4)}
  }

}
