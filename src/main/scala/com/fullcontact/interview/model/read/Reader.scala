package com.fullcontact.interview.model.read

import org.apache.spark.sql.{Dataset, SparkSession}

object Reader {
  def textReader(inPath: String)(implicit spark: SparkSession): Dataset[String] = spark.read.textFile(inPath)
}
