package com.fullcontact.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{Assertion, FunSuite, Matchers}

class FinderSparkJobTest extends FunSuite with Matchers {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  val log = Logger.getLogger(this.getClass)

  test("Validate RDDs for Output1 and Output2") {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val finderSparkJob = new FinderSparkJob(sparkSession)

    val testQueryPath = "./src/test/resources/testQueries.txt" //getClass.getResource("testQueries.txt").getPath()
    val testRecordsPath = "./src/test/resources/testRecords.txt" //getClass.getResource("testRecords.txt").getPath()
    log.info(s"testQueryPath [$testQueryPath]")
    log.info(s"testRecordsPath [$testRecordsPath]")

    val (output1Rdd, output2Rdd) = finderSparkJob.getOutputRdds(testQueryPath,testRecordsPath)

    val result1 = output1Rdd
      .collect()
      .sortBy(_._1)
      .map{case (k,v) => (k, v.sorted.toList)}

    val result2 = output2Rdd
      .collect()
      .sortBy(_._1)
      .map{case (k,v) => (k, v.sorted.toList)}

    output1Validation(result1.toList)
    output2Validation(result2.toList)
  }

  //EXPECTED RESULTS -
//    AAAAAAA -> AAAAAAA CCCCCCC, BBBBBBB AAAAAAA AAAAAAA
//    BBBBBBB -> BBBBBBB AAAAAAA AAAAAAA
//    CCCCCCC -> AAAAAAA CCCCCCC, CCCCCCC,
//    DDDDDDD -> DDDDDDD, DDDDDDD



  /**
   * Don't want to spend too much time on this larger set.  Asserting on counts but with more time
   * I would assert on each list.
   * @param out1
   * @return
   */
  def output1Validation(out1:  List[(String, List[String])]): List[Assertion] = {
    assert(out1.size.equals(8))

    val groupedOut = out1.groupBy(_._1)
    groupedOut.map{case (key, valuesList) =>
      key match {
        case "AAAAAAA" =>
          assert(valuesList.size.equals(3))
        case "BBBBBBB" =>
          assert(valuesList.size.equals(1))
        case "CCCCCCC" =>
          assert(valuesList.size.equals(2))
        case "DDDDDDD" =>
          assert(valuesList.size.equals(2))
        case _ =>
          log.error("key found not expected in output")
          assert(false)
      }
    }.toList
  }

  def output2Validation(out2:  List[(String, List[String])]): List[Assertion] = {
    assert(out2.size.equals(4))

    out2.map{ case (key, values) =>
      key match {
        //all values previously sorted in caller
        case "AAAAAAA" =>
          assert(values.equals(List("AAAAAAA", "BBBBBBB", "CCCCCCC")))
        case "BBBBBBB" =>
          assert(values.equals(List("AAAAAAA", "BBBBBBB")))
        case "CCCCCCC" =>
          assert(values.equals(List("AAAAAAA", "CCCCCCC")))
        case "DDDDDDD" =>
          assert(values.equals(List("DDDDDDD")))
        case _ =>
          log.error("key found not expected in output")
          assert(false)
      }
    }
  }
}
