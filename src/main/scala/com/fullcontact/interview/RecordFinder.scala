package com.fullcontact.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import scala.reflect.io.Directory
import java.io.File
import org.apache.spark.sql.SparkSession

object RecordFinder {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  val log = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    if(args.size != 2)
    {
      println("Need 2 arguments <Queries_File> <Records_File>")
      System.exit(1)
    }

    try {
      clearOutputDirs(Seq(Config.OUTPUT_LOCATION1,Config.OUTPUT_LOCATION2))

      val sparkConf = new SparkConf()
        .set("spark.hadoop.validateOutputSpecs", "false") //bit redundant with deleting dirs

      val sparkSession = SparkSession.builder()
        .master("local[*]")
        .config(sparkConf)
        .getOrCreate()

      //This does all the work.
      val finderSparkJob = new FinderSparkJob(sparkSession)
      finderSparkJob.genOutput(
        queryPath = args(0),
        recordPath = args(1),
        outputLocation1 = Config.OUTPUT_LOCATION1,
        outputLocation2 = Config.OUTPUT_LOCATION2,
        numFilePartitions = 1
      )

    } catch {
      case e: Exception =>
        log.error("Failure when starting job", e)
    }

    log.info(s"Time taken in ms [${System.currentTimeMillis() - startTime}]")
  }

  /**
   * Deleting output dirs because if we run this with less partitions we will leave extra partition files laying around.
   * @param dirs
   */
  def clearOutputDirs(dirs: Seq[String]): Unit = {
    dirs.foreach{ dirPath =>
      val directory = new Directory(new File(dirPath))
      directory.deleteRecursively()
    }
  }
}
