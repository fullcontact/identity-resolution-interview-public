package com.fullcontact.interview

import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object RecordFinder {
  def main(args: Array[String]): Unit = {
    // provide defaults for running without arguments
    var recordsFile: String = "Records.txt"
    var queriesFile: String = "Queries.txt"
    var output1File: String = "Output1.txt"
    var output2File: String = "Output2.txt"

    if(args.length == 4){
      recordsFile = args(0)
      queriesFile = args(1)
      output1File = args(2)
      output2File = args(3)
    } else if (args.length > 0){
      printHelp()
    }

    // build spark session
    val sparkConf = new SparkConf(true).setMaster("local[*]").setAppName("RecordFinder")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // build graph based on input records:
    // vertices: unique ids
    // edges: record strings that contain the connected vertices
    val recordGraph: Graph[String, String] = GraphBuilder.buildFromFile(sc, recordsFile)

    val queries: Dataset[String] = spark.read.textFile(queriesFile)
    val queryVertices: VertexRDD[String] = VertexRDD(queries.map(id => (IdGenerator.hash(id), id)).rdd)

    val graphLookup = new GraphLookup(recordGraph)

    val edgeQuery = graphLookup.getEdges(queryVertices)
    writeDf(edgeQuery.toDF(), output1File)

    val neighborsQuery = graphLookup.getNeighbors(queryVertices)
    writeDf(neighborsQuery.toDF(), output2File)
  }

  def writeDf(df: DataFrame, fileName: String): Unit = {
    df.coalesce(1).write.mode("overwrite").text(fileName)
  }

  def printHelp(): Unit = {
    System.out.print("RecordFinder required arguments:\n" +
    "1. Records file: space delineated ids representing possible association.\n" +
    "2. Queries file: each line contains a single id that we want to find associated records for.\n" +
    "3. Ouptut1 directory: Directory for output 1 (records for each query)\n" +
    "4. Output2 directory: Directory for output 2 (neighbors for each query)")
  }
}
