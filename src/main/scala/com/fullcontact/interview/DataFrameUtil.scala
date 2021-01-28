package com.fullcontact.interview

import com.fullcontact.interview.ColumnNames.{DEFAULT_COLUMN_NAME, ID, IDENTIFIER, IDENTIFIERS, IDENTIFIERS_ARR, POS}
import com.fullcontact.interview.IOUtil.readFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, array_distinct, col, collect_list, concat_ws, flatten, length, posexplode, sort_array, split, upper}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object DataFrameUtil {

  def explodeRecords(records: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    //redundant
//    val schema: StructType = records.schema.add(StructField(ID, LongType))
//    // Convert DataFrame to RDD and then call zipWithIndex
//    val dfRDD: RDD[(Row, Long)] = records.rdd.zipWithIndex()
//    val rowRDD: RDD[Row] = dfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
//    // Convert the indexed RDD into a DataFrame
//    val zippedRecords = sparkSession.createDataFrame(rowRDD, schema)

    val explodedZippedRecords = records.select(col("*"), posexplode(col(IDENTIFIERS_ARR)))
      .withColumnRenamed("col", IDENTIFIER)

    explodedZippedRecords
  }

  def readQueries(queryPath: String)(implicit sparkSession: SparkSession) = {
    readFile(queryPath)
      .withColumnRenamed(DEFAULT_COLUMN_NAME, IDENTIFIER)
      .filter(length(col(IDENTIFIER)) === 7)
      .filter(col(IDENTIFIER) === upper(col(IDENTIFIER)))
  }

  def readRecords(recordPath: String)(implicit sparkSession: SparkSession) = {
    readFile(recordPath)
      .withColumnRenamed(DEFAULT_COLUMN_NAME, IDENTIFIERS)
      .withColumn(IDENTIFIERS_ARR, split(col(IDENTIFIERS), "\\W"))
  }

  def joinQueryWithRecord(queries: DataFrame, records: DataFrame) = {
    var joined = queries.join(right = records, array_contains(col(IDENTIFIERS_ARR), col(IDENTIFIER)))
    joined = joined.cache()
    joined
  }

  def joinQueryWithRecordExploded(queries: DataFrame, records: DataFrame) = {
    var joined = queries.join(records, IDENTIFIER)
    joined = joined.cache()
    joined
  }


  def produceOutput1(joined: Dataset[Row]) = {
    joined
      .drop(IDENTIFIERS_ARR)
  }

  def produceOutput2(joined: Dataset[Row]) = {
    joined
      .groupBy(IDENTIFIER)
      .agg(collect_list(IDENTIFIERS_ARR).alias(IDENTIFIERS_ARR))
      .withColumn(IDENTIFIERS_ARR, concat_ws(" ", sort_array(array_distinct(flatten(col(IDENTIFIERS_ARR))))))
      .drop(IDENTIFIERS)
  }

  def produceOutput1Exploded(joined: Dataset[Row]) = {
    joined
      .drop(POS, IDENTIFIERS_ARR)
  }

  def produceOutput2Exploded(joined: Dataset[Row]): DataFrame = {
    joined
      .groupBy(IDENTIFIER)
      .agg(collect_list(IDENTIFIERS_ARR).alias(IDENTIFIERS_ARR))
      .withColumn(IDENTIFIERS_ARR, concat_ws(" ", sort_array(array_distinct(flatten(col(IDENTIFIERS_ARR))))))
      .drop(IDENTIFIERS, POS)
      .select(IDENTIFIER, IDENTIFIERS_ARR)
  }


}
