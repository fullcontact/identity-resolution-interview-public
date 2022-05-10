package com.fullcontact.interview

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.Try


class RecordFinderTest extends FunSuite with Matchers {

  val sc = SparkSession.builder()
    .appName("spark testing")
    .master("local")
    .getOrCreate()

  test("that recordDataProcessor method returns a row for every identifier for records") {

    val recordListBuffer1 = new ListBuffer[String]()
    recordListBuffer1+=("FASXGPW", "UMFRDAA")
    val recordList1= recordListBuffer1.toArray

    val recordListBuffer2 = new ListBuffer[String]()
    recordListBuffer2+=("AAQXNZT","AOQWEJU","JOEQGHG")
    val recordList2 = recordListBuffer2.toArray

    val rdd = sc.sparkContext.parallelize(Array(recordList1,recordList2))
    val d = RecordFinder.recordDataProcessor(rdd)

    assert(5==d.count())
  }

  test("that left outer join joins queries and records and return distinct entries") {
    val recordListBuffer1 = new ListBuffer[String]()
    recordListBuffer1+=("FASXGPW", "UMFRDAA")
    val recordList1= recordListBuffer1.toArray
    val recordListBuffer2 = new ListBuffer[String]()
    recordListBuffer2+=("AAQXNZT","AOQWEJU","JOEQGHG", "FASXGPW")
    val recordList2 = recordListBuffer2.toArray

    val queriesListBuffer = new ListBuffer[String]()
    queriesListBuffer+=("FASXGPW", "FASXGPW")
    val queriesList= queriesListBuffer.toArray
    val queriesRdd = sc.sparkContext.parallelize(queriesList).map(x => (x, None))

    val recordRdd = RecordFinder.recordDataProcessor(sc.sparkContext.parallelize(Array(recordList1,recordList2)))
    val resArr= RecordFinder.transformer(queriesRdd, recordRdd).map{case(key, value) => key + value.mkString("", " ", "")}.collect()

    assert(resArr.length == 2)
    assert(1 == resArr.count(x => x.equals("FASXGPW:FASXGPW UMFRDAA")) )
    assert(1 == resArr.count(x =>x.equals("FASXGPW:AAQXNZT AOQWEJU JOEQGHG FASXGPW")))
  }

  test("that when there is no matching to identity in queries file, identity still returns but with empty content") {

    val recordListBuffer = new ListBuffer[String]()
    recordListBuffer+=("ERERER")
    val recordList = recordListBuffer.toArray

    val queriesListBuffer = new ListBuffer[String]()
    queriesListBuffer+=("FASXGPW", "DXYGHLN")
    val queriesList= queriesListBuffer.toArray

    val queriesRdd = sc.sparkContext.parallelize(queriesList).map(x => (x, None))
    val recordRdd = RecordFinder.recordDataProcessor(sc.sparkContext.parallelize(Array(recordList)))
    val resArr= RecordFinder.transformer(queriesRdd, recordRdd).map{case(key, value) => key + value.mkString("", " ", "")}.collect()

    assert(2 == resArr.length)
    assert(1 == resArr.count(x => x.equals("DXYGHLN:")))
    assert(1 == resArr.count(x =>x.equals("FASXGPW:")))
  }

  test("that transformByDistinctKey() method returns unique keys with combined distinct values") {

    val recordListBuffer1 = new ListBuffer[String]()
    recordListBuffer1+=("FASXGPW", "UMFRDAA", "GGGG")
    val recordList1= recordListBuffer1.toArray

    val recordListBuffer2 = new ListBuffer[String]()
    recordListBuffer2+=("AAQXNZT","AOQWEJU","JOEQGHG", "FASXGPW")
    val recordList2 = recordListBuffer2.toArray

    val queriesListBuffer = new ListBuffer[String]()
    queriesListBuffer+=("FASXGPW", "FASXGPW", "GGGG")
    val queriesList= queriesListBuffer.toArray

    val queriesRdd = sc.sparkContext.parallelize(queriesList).map(x => (x, None))
    val recordRdd = RecordFinder.recordDataProcessor(sc.sparkContext.parallelize(Array(recordList1,recordList2)))
    val transformedRdd= RecordFinder.transformer(queriesRdd, recordRdd)
    val resArr = RecordFinder.transformByDistinctKey(transformedRdd).map{case(key, value) => key + value.mkString("", " ", "")}.collect()

    assert(2 == resArr.length)
    assert("FASXGPW:GGGG AOQWEJU JOEQGHG UMFRDAA FASXGPW AAQXNZT".equals(resArr(0)))
    assert("GGGG:GGGG UMFRDAA FASXGPW".equals(resArr(1)))
  }

  test("test") {
    Reader.recordReader("jjjj", sc.sparkContext)
  }
}
