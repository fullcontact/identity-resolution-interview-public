package com.fullcontact.interview.utils

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TestUtils {
  def compareDatasets[T](actual: Dataset[T], expected: Dataset[T]): Unit = {
    // Set nullable in StructField to true to avoid schema discrepancies
    // We only worry about name and type to match
    val newActualSchema = setAllFieldsNullable(actual.schema)
    val newExpectedSchema = setAllFieldsNullable(expected.schema)

    assert(newActualSchema.equals(newExpectedSchema))
    assert(actual.collect().sameElements(expected.collect()))
  }

  def unorderedCompareDatasets[T](actual: Dataset[T], expected: Dataset[T], sortCols: Seq[String]): Unit = {
    val actualSorted = actual.sort(sortCols.head, sortCols.tail: _*)
    val expectedSorted = expected.sort(sortCols.head, sortCols.tail: _*)
    compareDatasets(actualSorted, expectedSorted)
  }

  def setAllFieldsNullable(schemaInput: StructType) = {
    StructType(schemaInput.map {
      case StructField(c, t, _, m) => StructField(c, t, nullable = true, m)
    })
  }

  def convertToDataframe(spark: SparkSession, rows: List[Row], schema: StructType) = {
    val rdd = spark.sparkContext.makeRDD(rows)
    spark.sqlContext.createDataFrame(rdd, schema)
  }
}
