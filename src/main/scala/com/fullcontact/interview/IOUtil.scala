package com.fullcontact.interview

import java.io.File

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object IOUtil {

  def verifyInputFileExists(queryPath: String, messagePrefix: String): Boolean = {
    try {
      if (!new File(queryPath).exists()) {
        print(s"$messagePrefix: $queryPath, does not exists")
        return false
      }
    } catch {
      case e: Exception => print(e.getMessage); return false;
    }
    true
  }

  def readFile(path: String)(implicit sparkSession: SparkSession) = {
    println(s"Reading data from ${path}")
    val records = sparkSession.read
      .option("header", false)
      .option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "corrupt_record")
      .csv(path)

    records
  }

  def saveOutput(data: DataFrame, location: String) {
    println(s"Saving results into: ${location}")
    data.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", ":")
      .csv(location)
  }

}
