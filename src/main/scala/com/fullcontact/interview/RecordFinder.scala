package com.fullcontact.interview
import com.fullcontact.interview.Helper._
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

import java.io.File



object RecordFinder {
  def main(args: Array[String]): (Array[String], Long, Long) = {
    // TODO: Implement your job here
    val DataDirPath: String = System.getProperty("user.dir")
    val queriesDataPath: String = DataDirPath.concat("/Queries.txt")
    val recordsDataPath: String = DataDirPath.concat("/Records.txt")
    println(queriesDataPath)
    println(recordsDataPath)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    spark.conf.set("spark.sql.broadcastTimeout",48000)




//    queriesRDD.take(10).foreach(println(_))

    val recordsRDD: RDD[Array[String]] = spark.sparkContext
      .textFile(recordsDataPath)
      .map(_.trim)
      .map(line => line.split(" "))

//    recordsRDD.take(10).foreach(println(_))

    val connectionsRDD: RDD[Array[String]] = recordsRDD.flatMap(
      ls =>  createPairs(ls.head, getNeighbors(ls) )
    )

//    connectionsRDD.take(10).foreach(x => println(x.mkString(" "))
    val recordsEdgesRDD: RDD[Edge[String]] = connectionsRDD.map(c =>
      Edge(asciiToLong(c.head)
        , asciiToLong(c.last)
        , c.head + ":" + c.last
      )
    )


    val graphData = recordsRDD.flatMap( v => v )

    val vertexData: RDD[(Long, (String, String))] = graphData.map( v => (asciiToLong(v), (v, v)) )

//    vertexData.foreach(v => println(v._2))

    val graph = Graph(vertexData, recordsEdgesRDD)
    val recordsGraph = Graph.fromEdges(recordsEdgesRDD, 1)

    val vtx = graph.vertices

    val cc = graph.connectedComponents()

    val vertexToAttributes = vtx.map( v => (v._1, v._2._1))
    val vertexToAttributesCollect  = vertexToAttributes.collect()

//    vertexToAttributesCollect.foreach(v => println(v.toString()))

    val connectedCompVertices : VertexRDD[VertexId] = cc.vertices
    val connectedCompIdsToVertices = connectedCompVertices.groupBy(_._2)

//    connectedCompIdsToVertices.foreach(x => println(x.toString()))



//    val joinQueriesRecords = longToQueriesAsciiRDD.leftOuterJoin(connectedCompIdsToVertices)
//
//    joinQueriesRecords.foreach(v => println(v.toString()))

    val connectedCompIdsToVerticesRDD = connectedCompIdsToVertices.
      map(
        x => (x._1
          , x._2.flatMap(t => Array(t._1, t._2))
        )
      )

//    connectedCompIdsToVerticesCollect.take(200).foreach(v => println(v))

    val connectedCompsRecordsGraph = recordsGraph.connectedComponents()

    val connectedComponentsByIDs = connectedCompsRecordsGraph.vertices.groupBy(v => v._2).distinct()
    //    val numComponents = connectedComponentsByIDs.count()

    val queriesRDD: RDD[String] = spark.sparkContext
      .textFile(queriesDataPath)
      .map(_.trim)

    val queriesPairRDD = queriesRDD.map( c => (c, asciiToLong(c)))
    val numQueryItems = queriesRDD.count()
    val numQueryLongItems = queriesPairRDD.map(_._2).distinct().count()


//    val longToQueriesAsciiRDD: RDD[(Long, String)] = queriesRDD.map(q => ( asciiToLong(q), q))

    val queriesAsciiItems = queriesRDD.map(c => asciiToLong(c)).distinct()


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

    println("making the join")

    val queryJoin = queriesPairRDD.join(relationsAscii)

    println("printing the join")
    queryJoin.take(100).foreach( p => println(p))

    println("print-join done")

    val queriedRelations = queryJoin.map(
      q => (q._1
        , q._2._2
        )
    )

    val queriedRelationsOutput = queriedRelations.map(
      q => q._1.concat(":"+q._2.mkString(" "))

    )

    queriedRelationsOutput.take(10).foreach(p => println(p))

//    val relationsAsciiOutput = relationsAscii.map(
//      x =>
//        x._1.concat(":"+x._2.mkString(" "))
//    )

//    relationsAscii.take(10).foreach(c => println(c.toString()))

    val outputPath: String = DataDirPath.concat("/Matches.txt")
    println("Saving results to Matches.txt...")

    queriedRelationsOutput.coalesce(3).saveAsTextFile(outputPath)



    spark.stop()

    (queriedRelationsOutput.take(1), numQueryItems, numQueryLongItems)

  }
}
