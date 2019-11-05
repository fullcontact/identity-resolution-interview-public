package com.fullcontact.interview

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class RecordFinderTest extends FunSuite with Matchers with BeforeAndAfter {
  var sparkSession: SparkSession = _

  test("Validate exploding records dataframe by each identifier") {
    val spark = sparkSession

    import spark.implicits._

    val records = Seq(
      "A B C",
      "D E F",
      "A C D"
    ).toDF(RecordFinder.Record)

    val expected = Seq(
      ("A", "A B C"),
      ("B", "A B C"),
      ("C", "A B C"),
      ("D", "D E F"),
      ("E", "D E F"),
      ("F", "D E F"),
      ("A", "A C D"),
      ("C", "A C D"),
      ("D", "A C D"),
    ).toDF(RecordFinder.Identifier, RecordFinder.Record)

    val exploded = RecordFinder.explodeRecordsDF(records)

    assert(
      expected.as[(String, String)].collect() sameElements exploded.as[(String, String)].collect(),
      "The exploded dataframe was not the same as the expected value"
    )

  }

  test("Validate grouping exploded dataframe by identifier") {
    val spark = sparkSession
    import spark.implicits._


    val expected = Seq(
      ("A", "A B C D"),
      ("B", "A B C"),
      ("C", "A B C D"),
      ("D", "A C D E F"),
      ("E", "D E F"),
      ("F", "D E F")
    ).toDS()
      .as[(String, String)]
      .map {
        case (id: String, record: String) => {
          (id, record.split(" ").sorted.mkString(" "))
        }
      }


    val exploded = Seq(
      ("A", "A B C"),
      ("B", "A B C"),
      ("C", "A B C"),
      ("D", "D E F"),
      ("E", "D E F"),
      ("F", "D E F"),
      ("A", "A C D"),
      ("C", "A C D"),
      ("D", "A C D"),
    ).toDF(RecordFinder.Identifier, RecordFinder.Record)

    val actual = RecordFinder.groupRecordsByIdDF(exploded)
      .as[(String, String)]
      .map {
        case (id: String, record: String) => {
          (id, record.split(" ").sorted.mkString(" "))
        }
      }.sort(org.apache.spark.sql.functions.asc("_1"))

    assert(
      expected.as[(String, String)].collect() sameElements actual.as[(String, String)].collect(),
      "The exploded dataframe was not the same as the expected value"
    )

  }

  before {
    sparkSession = SparkSession.builder().master("local[4]").getOrCreate()
  }


}
