package com.fullcontact.interview
import com.fullcontact.interview.Helper._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._



object RecordFinder {
  def main(args: Array[String]): (Array[String], Long, Long) = {
    // TODO: Implement your job here
    val DataDirPath: String = System.getProperty("user.dir")
    val queriesDataPath: String = DataDirPath.concat("/Queries.txt")
    val recordsDataPath: String = DataDirPath.concat("/Records.txt")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    spark.conf.set("spark.sql.broadcastTimeout",48000)

    val recordsRDD: RDD[Array[String]] = spark.sparkContext
      .textFile(recordsDataPath)
      .map(_.trim)
      .map(line => line.split(" "))

    val connectionsRDD: RDD[Array[String]] = recordsRDD.flatMap(
      ls =>  createPairs(ls.head, getNeighbors(ls) )
    )

    val recordsEdgesRDD: RDD[Edge[String]] = connectionsRDD.map(c =>
      Edge(asciiToLong(c.head)
        , asciiToLong(c.last)
        , c.head + ":" + c.last
      )
    )

    val graphData = recordsRDD.flatMap( v => v )
    val vertexData: RDD[(Long, (String, String))] = graphData.map( v => (asciiToLong(v), (v, v)) )

    val graph = Graph(vertexData, recordsEdgesRDD)
    val vtx = graph.vertices
    val cc = graph.connectedComponents()

    val vertexToAttributes = vtx.map( v => (v._1, v._2._1))
    val vertexToAttributesCollect  = vertexToAttributes.collect()

    val connectedCompVertices : VertexRDD[VertexId] = cc.vertices
    val connectedCompIdsToVertices = connectedCompVertices.groupBy(_._2)

    val connectedCompIdsToVerticesRDD = connectedCompIdsToVertices.
      map(
        x => (x._1
          , x._2.flatMap(t => Array(t._1, t._2))
        )
      )

    val queriesRDD: RDD[String] = spark.sparkContext
      .textFile(queriesDataPath)
      .map(_.trim)

    val queriesPairRDD = queriesRDD.map( c => (c, asciiToLong(c)))
    val numQueryItems = queriesRDD.count()
    val numQueryLongItems = queriesPairRDD.map(_._2).distinct().count()

    val connectedCompIdsToVerticesAscii:RDD[(String, Iterable[String])] = connectedCompIdsToVerticesRDD.
      map(
        x => (vertexToAttributesCollect.filter( vc => vc._1 == x._1).map(_._2).head
          , x._2.map( cd => vertexToAttributesCollect.filter( pc => pc._1 == cd).map(_._2).head)
          )
      )

    val relationsAscii  = connectedCompIdsToVerticesAscii.map(
      x => (
        x._1,
        x._2.filter(c => c != x._1)
      )
    )

    relationsAscii.persist()
    queriesRDD.persist()

    val queryJoin = queriesPairRDD.join(relationsAscii)

    queryJoin.take(100).foreach( p => println(p))

    val queriedRelations = queryJoin.map(
      q => (q._1
        , q._2._2
        )
    )

    val queriedRelationsOutput = queriedRelations.map(
      q => q._1.concat(":"+q._2.mkString(" "))

    )

    queriedRelationsOutput.take(10).foreach(p => println(p))

    val outputPath: String = DataDirPath.concat("/Matches.txt")
    println("Saving results to Matches.txt...") // write to logger

    queriedRelationsOutput.coalesce(3).saveAsTextFile(outputPath)

//    spark.stop()

    (queriedRelationsOutput.take(1), numQueryItems, numQueryLongItems)

  }
}
