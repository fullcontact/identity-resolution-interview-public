package com.fullcontact.interview

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object RecordFinder {

  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Main class used running app
   * <p>
   *
   * @param args
   */
  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    // TODO: Implement your job here

    // SOME SHORTCUTS FOR TEST OTHERWISE ADDED IN ARGUMENTS
    val isLocalMode = true
    val appName = "RecordFinderTest"
    val recordLocation = "Records.txt"
    val queryLocation = "Queries.txt"
    val unmergedSearchResultsLocation = "./Output1.txt"
    val mergedSearchResultsLocation = "./Output2.txt"

    try {
      val sparkBuilder = SparkSession.builder.appName(appName)
      if (isLocalMode) {
        sparkBuilder.master("local")
      }
      implicit val spark: SparkSession = sparkBuilder.getOrCreate()

      val searchIdDF =  createSearchIdDF(spark, queryLocation)
      val idTableDf = createIdTableDF(spark, recordLocation)


      if (isLocalMode) {
        spark.stop()
      }
    } catch {
      case e: Throwable => {
        LOGGER.error(s"${appName} application failure: ", e)
        val exc = e
        throw e
      }
    } finally {
      LOGGER.info(s"Completed ${appName}")
    }

  }

  def createSearchIdDF(spark: SparkSession, location: String): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    var df: DataFrame = spark.read.text(location)
    df =  df
      .withColumn("searchkey", $"value")
      .drop("value")
    df
  }

  /**
   * Create a dataframe of all the records used for query
   *
   * @param spark
   * @param location
   * @return
   */
  def createIdTableDF(spark: SparkSession, location: String): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    var df: DataFrame = spark.read.text(location)

    df.show(3)

//    df =  df
//      .withColumn("key", split($"value", " "))
//      .withColumn("idmap", map_from_arrays($"key", $"key"))
//      .drop("key")

    df =  df.select($"value", split(col("value"),",").as("idarray"))

    df.show(3)

    df

  }

  /**
   * Searches the id dataframe to retrieve records that contain the searched id, unmerged
   * @param searchDF
   * @param idDataFrame
   * @return
   */
    def searchUnMergedRecords(searchDF: DataFrame, idDataFrame: DataFrame): DataFrame = {

      // check out array_contains
      val thisSpark: SparkSession = searchDF.sparkSession
      import thisSpark.implicits._
      searchDF.createOrReplaceTempView("searchvals")
      idDataFrame.createOrReplaceTempView("id_table")
      thisSpark.sql("select ")
    }

  //  /**
  //   * Persiste
  //   */
  //  def persistUnmergedRecord(unmergedSearchDf: DataFrame, persistLocation: String): Unit = {
  //
  //  check out combineByKey
  //  and array_distinct
  //  }

}
