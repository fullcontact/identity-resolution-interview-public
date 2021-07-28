package com.fullcontact.interview

import com.fullcontact.interview.model.process.Processor
import com.fullcontact.interview.model.read.Reader.textReader
import com.fullcontact.interview.model.write.Writer.textWriter
import com.fullcontact.interview.util.DefaultSparkInit
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object RecordFinder extends App {

  implicit val spark: SparkSession = new DefaultSparkInit().createSession()
  Try {
    val recordsLinePath = args(0)
    val queriesPath = args(1)
    val outputPath = args(2)
    val queriesDf = textReader(queriesPath).toDF("query_id")
    val recordsDf = textReader(recordsLinePath).toDF("record_ids")
    val (processedDfWithDuplicates, processedDfWithoutDuplicatesDf) = Processor(recordsDf, queriesDf)
    textWriter(processedDfWithDuplicates, s"$outputPath/processed_with_duplicates")
    textWriter(processedDfWithoutDuplicatesDf, s"$outputPath/processed_without_duplicates")
  } match {
    case Success(_) => spark.stop()
    case Failure(exception) =>
      spark.stop()
      throw exception
  }
}
