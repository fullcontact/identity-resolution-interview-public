package com.fullcontact.interview

import com.fullcontact.interview.DataFrameUtil._
import com.fullcontact.interview.ColumnNames.{IDENTIFIER, IDENTIFIERS, IDENTIFIERS_ARR}
import org.apache.spark.sql.functions.{col, split}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RecordFinderTest extends FunSuite with Matchers with BeforeAndAfter {
  implicit var sparkSession: SparkSession = _

  before {
    sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  }

  test("test query to records matching") {


    val spark = sparkSession

    import spark.implicits._

    val queries = Seq(
      "1",
      "2",
      "3",
      "4",
      "5"
    ).toDF(IDENTIFIER)

    val records = Seq(
      ("1 2 3"),
      ("1 2 3"),
      ("1 2 3"),
      ("2 15"),
      ("4 5 6"),
      ("4 5 6"),
      ("5 19"),
      ("3 4"),
      ("1 3"),
    ).toDF(IDENTIFIERS)
      .withColumn(IDENTIFIERS_ARR, split(col(IDENTIFIERS), "\\W"))


    val expectedOutput1 = Seq(
      ("1", "1 2 3"),
      ("1", "1 2 3"),
      ("1", "1 2 3"),
      ("1", "1 3"),
      ("2", "1 2 3"),
      ("2", "1 2 3"),
      ("2", "1 2 3"),
      ("2", "2 15"),
      ("3", "1 2 3"),
      ("3", "1 2 3"),
      ("3", "1 2 3"),
      ("3", "3 4"),
      ("3", "1 3"),
      ("4", "4 5 6"),
      ("4", "4 5 6"),
      ("4", "3 4"),
      ("5", "4 5 6"),
      ("5", "4 5 6"),
      ("5", "5 19")
    ).toDF(IDENTIFIER, IDENTIFIERS)


    val expectedOutput2 = Seq(
      ("1", "1 2 3"),
      ("2", "1 15 2 3"),
      ("3", "1 2 3 4"),
      ("4", "3 4 5 6"),
      ("5", "19 4 5 6")
    ).toDF(IDENTIFIER, IDENTIFIERS)

    val joined = joinQueryWithRecord(queries, records)
    val producedOutput1 = produceOutput1(joined)
    val producedOutput2 = produceOutput2(joined)

    val explodedZippedRecords = explodeZippedRecords(records)
    var joinedExploded = joinQueryWithRecordExploded(queries, explodedZippedRecords)
    val producedOutput1Exploded = produceOutput1Exploded(joinedExploded)
    val producedOutput2Exploded = produceOutput2Exploded(joinedExploded)

    assert(assertDataFrameEquality(producedOutput1, expectedOutput1))
    assert(assertDataFrameEquality(producedOutput1Exploded, expectedOutput1))

    assert(assertDataFrameEquality(producedOutput2, expectedOutput2))
    assert(assertDataFrameEquality(producedOutput2Exploded, expectedOutput2))

  }

  def assertDataFrameEquality(actual: DataFrame, expected: DataFrame): Boolean = {
    //actual.union(expected).distinct().count() == actual.intersect(expected).count()
    val diff = expected.except(actual)
    if (!expected.except(actual).isEmpty) {
      println("actual \n")
      actual show()
      println("expected \n")
      actual show()
      println("difference \n")
      diff show()
    }

    expected.except(actual).isEmpty
  }
}
