package com.fullcontact.interview

import com.fullcontact.interview.arguments.{CommandArgs, CommandArgsParser}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object RecordFinder {

  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
  private var cmdConfigParser: CommandArgsParser = new CommandArgsParser()

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

    try {
      if (args == null || args.isEmpty) {
        throw new Exception(s"${appName} did not recieve any arguments")
      } else {
        LOGGER.info(s"Arguments: ${args.mkString(",")}")

        val configOption: Option[CommandArgs] = cmdConfigParser.parse(args.toSeq, CommandArgs())
        if (configOption.isEmpty) {
          throw new Exception("AudienceEnrichmentS3Loader could not parse arguments")
        } else {
          val config: CommandArgs = configOption.get
          LOGGER.info(s"Starting application ${appName}")

          val sparkBuilder = SparkSession.builder.appName(appName)
          if (isLocalMode) {
            sparkBuilder.master("local")
          }
          implicit val spark: SparkSession = sparkBuilder.getOrCreate()

          val searchIdDF: DataFrame = createSearchIdDF(spark, config.queryFilePath)
          val idTableDf: DataFrame = createIdTableDF(spark, config.recordFilePath)
          val unmergedRawDF: DataFrame = searchUnMergedRecords(searchIdDF, idTableDf)
          val unmergedFormattedDF: DataFrame = formatOutPutDF(unmergedRawDF)
          val mergedRawDF: DataFrame = mergeArraysBySearchID(unmergedRawDF)
          val mergedFormattedDF: DataFrame = formatOutPutDF(mergedRawDF)
          persisteTextFile(unmergedFormattedDF, config.unmergedOutputPath)
          persisteTextFile(mergedFormattedDF, config.mergedOutputPath)


          if (isLocalMode) {
            spark.stop()
          }

          LOGGER.info(s"Stopped application ${appName}")
        }
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


  /**
   * Creates a dataframe from the search id file
   *
   * @param spark
   * @param location
   * @throws java.lang.Exception
   * @return
   */
  @throws(classOf[Exception])
  def createSearchIdDF(spark: SparkSession, location: String): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    var df: DataFrame = spark.read.text(location)
    df = df
      .withColumn("searchkey", trim($"value"))
      .drop("value")
      .dropDuplicates()
    df.show(3)
    df
  }

  /**
   * Create a dataframe of all the records used for query
   *
   * @param spark
   * @param location
   * @return
   */
  @throws(classOf[Exception])
  def createIdTableDF(spark: SparkSession, location: String): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    var df: DataFrame = spark.read.text(location)

    df.show(3)
    // FIRST THOUGHT, USE A MAP WHICH WOULD HOPEFULLY BE FASTER THAN AN ARRAY TO SEARCH
    //    df =  df
    //      .withColumn("key", split($"value", " "))
    //      .withColumn("idmap", map_from_arrays($"key", $"key"))
    //      .drop("key")

    df = df.withColumn("trimmed", trim($"value"))
    df = df.select($"trimmed", split(col("value"), "\\s+").as("idarray"))
    df = df.drop("trimmed", "value")
    df.show(3)
    df
  }

  /**
   * Searches the id dataframe to retrieve records that contain the searched id, unmerged
   *
   * @param searchDF
   * @param idDataFrame
   * @return
   */
  @throws(classOf[Exception])
    def searchUnMergedRecords(searchDF: DataFrame, idDataFrame: DataFrame): DataFrame = {

    // check out array_contains
    val thisSpark: SparkSession = searchDF.sparkSession
    searchDF.createOrReplaceTempView("searchvals")
    idDataFrame.createOrReplaceTempView("id_table")
    val joinedDF = thisSpark.sql(
      """select s.searchkey, i.idarray
        |from id_table i, searchvals s
        |where array_contains(i.idarray, s.searchkey)
        |""".stripMargin)

    joinedDF.show(20)
    joinedDF
  }

  /**
   * Function to merge the duplicated rows with the same search id
   * @param df
   * @return
   */
  def mergeArraysBySearchID(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import df.sparkSession.implicits._

    var returnDF: DataFrame = df.groupBy("searchkey").agg(collect_set("idarray").as("idarraycombined"))
    returnDF = returnDF.withColumn("idarrayflattened", flatten($"idarraycombined"))
    returnDF =  returnDF.withColumn("idarray", array_distinct($"idarrayflattened"))
    returnDF = returnDF.drop("idarraycombined", "idarrayflattened")
    returnDF.show(20)

    returnDF
  }



  /**
   * Format the dataframe to fit the "key: key1 key2 key3" format
   * @param df
   * @return
   */
  def formatOutPutDF(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    var returnDF = df.withColumn("arraystring", concat_ws(" ", $"idarray"))
    returnDF = returnDF.select(concat($"searchkey", lit(':'), $"arraystring").as("outputline"))
    returnDF.show(10)
    returnDF
  }

  /**
   * Function to persist text file to a location
   * @param df
   * @param persistLocation
   */
  @throws(classOf[Exception])
    def persisteTextFile(df: DataFrame, persistLocation: String): Unit = {
      df.write.mode(SaveMode.Overwrite).text(persistLocation)
    }

}
