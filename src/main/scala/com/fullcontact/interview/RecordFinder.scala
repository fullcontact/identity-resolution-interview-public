package com.fullcontact.interview

import com.fullcontact.interview.RecordFinderArguments.RecordFinderConfig
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RecordFinder {

  def main(args: Array[String]): Unit = {

    val config: RecordFinderConfig = RecordFinderArguments.parse(args)

    //export SPARK_LOCAL_IP="127.0.0.1"
    val sparkSession: SparkSession = SparkSession
      .builder
      .appName("Identity Join job")
      .master("local[*]")
      .getOrCreate()

    val recordFinder: RecordFinder = new RecordFinder(sparkSession)

    // Read inputs
    val records: Dataset[RecordRow] = recordFinder.readRecords(config.recordsFile)
    val queries: Dataset[QueryRow] = recordFinder.readQueries(config.queriesFile)

    // Explode records into 1 row per identifier with rowId
    val explodedRecordsDS: Dataset[RecordRowExploded] = recordFinder.explodeRecords(records)

    // Join queries with exploded records and then with records to generate report1
    val identifierWithRowsReport: Dataset[IdentifierWithRow] = recordFinder
      .generateReport1(queries, explodedRecordsDS, records)

    // Save Report 1
    identifierWithRowsReport
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", ":")
      .csv(config.report1Dir)

    val sameIdentifiersReport: Dataset[IdentifierWithRelatedIds] = recordFinder.generateReport2(identifierWithRowsReport)

    // Save Report 2
    sameIdentifiersReport
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", ":")
      .csv(config.report2Dir)
  }
}

class RecordFinder(sparkSession: SparkSession) {

  import sparkSession.implicits._

  def readRecords(recordsFilePath: String): Dataset[RecordRow] = {
    val records: DataFrame = sparkSession.read.csv(recordsFilePath)
    records
      .withColumnRenamed("_c0", "identifiers")
      .withColumn("rowId", monotonically_increasing_id)
      .as[RecordRow]
  }

  def readQueries(queriesFilePath: String): Dataset[QueryRow] = {
    val queries: DataFrame = sparkSession.read.csv(queriesFilePath)
    queries
      .withColumnRenamed("_c0", "identifier")
      .as[QueryRow]
  }

  def explodeRecords(records: Dataset[RecordRow]): Dataset[RecordRowExploded] = {
    records
      .withColumn("identifier", explode(split('identifiers, " ")))
      .drop('identifiers)
      .as[RecordRowExploded]
  }

  def generateReport1(
    queries: Dataset[QueryRow],
    explodedRecordsDS: Dataset[RecordRowExploded],
    records: Dataset[RecordRow]): Dataset[IdentifierWithRow] = {
    queries
      .join(explodedRecordsDS, Seq("identifier"), "left_outer")
      .join(records, Seq("rowId"), "left_outer")
      .drop('rowId)
      .withColumnRenamed("identifiers", "rowWithIdentifier")
      .as[IdentifierWithRow]
  }

  def generateReport2(identifierWithRowsReport: Dataset[IdentifierWithRow]): Dataset[IdentifierWithRelatedIds] = {
    val idWithRelatedIdsSetDS: Dataset[(String, Set[String])] = identifierWithRowsReport
      .map(record => {
        val ids: Set[String] = record.rowWithIdentifier.fold(Array.empty[String])(_.split(" ")).toSet
        (record.identifier, ids)
      })

    // Group by query id and combine sets
    idWithRelatedIdsSetDS.map(x => x._1 -> x._2).rdd.reduceByKey(_ ++ _)
      .map(record => (record._1, record._2.mkString(" ")))
      .toDF("identifier", "relatedIdentifiers")
      .as[IdentifierWithRelatedIds]
  }
}
