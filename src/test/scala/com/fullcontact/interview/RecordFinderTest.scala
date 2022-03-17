package com.fullcontact.interview

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

class RecordFinderTest extends FunSuite with DataFrameSuiteBase with Matchers {

  override implicit def enableHiveSupport: Boolean = false

  test("convertRecordsToArrayAndThenExplode") {

    val data = Seq(
      Row("DJBEEPL FSNMWAF"),
      Row("FSNMWAF MLEAGKE")
    )
    val schema = new StructType()
      .add("value", StringType, true)

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val actualDF = RecordFinder.convertRecordsToArrayAndThenExplode(inputDF)

    val expectedSchema = new StructType()
      .add("value", StringType)
      .add("record", ArrayType(new StructType()))
      .add("Id", StringType)

    val expectedData =  Seq(
      Row("DJBEEPL FSNMWAF",
          List(
            Row("DJBEEPL"),
            Row("FSNMWAF")
          ),
        "DJBEEPL"
      ),
      Row("DJBEEPL FSNMWAF",
        List(
          Row("DJBEEPL"),
          Row("FSNMWAF")
        ),
        "FSNMWAF"
      ),
      Row("FSNMWAF MLEAGKE",
        List(
          Row("DJBEEPL"),
          Row("FSNMWAF")
        ),
        "FSNMWAF"
      ),
      Row("FSNMWAF MLEAGKE",
        List(
          Row("FSNMWAF"),
          Row("MLEAGKE")
        ),
        "MLEAGKE"
      )
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )
    actualDF.show(false)
    expectedDF.show(false)

    assertDataFrameNoOrderEquals(actualDF, expectedDF)
  }

  test("getRecordsForQuery") {

  }

  test("getAllRecords") {

  }

  test("getDedupRecords") {

  }

}
