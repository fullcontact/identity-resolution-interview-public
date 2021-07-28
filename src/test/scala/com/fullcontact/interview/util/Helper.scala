package com.fullcontact.interview.util

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, FileNotFoundException}

object Helper {

  def createDataframeFromList(inSeq: Seq[String], columnName: String)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    inSeq.toDF(columnName)
  }

  def getDataAsString(data: DataFrame, nullValue: String = null): Seq[Seq[String]] = {
    data.collect().map(_.toSeq.map(value => if (value != null) value.toString else nullValue))
  }

  def findResourcePath(path: String): String = findResourceFile(path).getAbsolutePath

  def findResourceFile(path: String): File = {
    Option(Thread.currentThread().getContextClassLoader.getResource(path))
      .map(url => new File(url.toURI))
      .getOrElse(throw new FileNotFoundException(path))
  }


}
