package com.fullcontact.interview
import com.fullcontact.interview.Helper._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._


object RecordFinder {
  def main(args: Array[String]): Unit = {
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

    val vtx = graph.vertices

    val cc = graph.connectedComponents()

    val vertexToAttributes = vtx.map( v => (v._1, v._2._1))

//    vertexToAttributes.foreach(v=> println(v)) //(85708268758575,UFRDKUK)

    val vertexToAttributesCollect  = vertexToAttributes.collect()



    val connectedComponentIds = cc.vertices.map( _._2).distinct()

//    vertexToAttributesCollect.foreach(println(_))

//    connectedComponentIds.foreach( x => println(x))

    val connectedCompVertices : VertexRDD[VertexId] = cc.vertices

    val connectedCompIdsToVertices = connectedCompVertices.groupBy(_._2)

//    connectedCompIdsToVertices.foreach(x => println(x.toString()))

    val queriesRDD: RDD[String] = spark.sparkContext
          .textFile(queriesDataPath)
          .map(_.trim)

    val longToQueriesAsciiRDD = queriesRDD.map(c => (asciiToLong(c), c) )

//    val joinQueriesRecords = longToQueriesAsciiRDD.leftOuterJoin(connectedCompIdsToVertices)

//    connectedCompIdsToVertices.foreach(v => println(v.toString()))

    //(77778878866683,CompactBuffer((87766679737070,77778878866683), (77778878866683,77778878866683)))

    val connectedCompIdsToVerticesRDD = connectedCompIdsToVertices.
      map(

        x => (x._1
          , x._2.flatMap(t => Array(t._1, t._2))
        )
      )

    // (76868984758878,List(78788288839085, 76868984758878, 76868984758878, 76868984758878, 82708667767572, 76868984758878))


    val connectedCompIdsToVerticesAscii = connectedCompIdsToVerticesRDD.
      map(

        x => (vertexToAttributesCollect.filter( vc => vc._1 == x._1).map(_._2).head
          , x._2.map( cd => vertexToAttributesCollect.filter( pc => pc._1 == cd).map(_._2).head)
          )
      )

//    connectedCompIdsToVerticesRDD.take(10).foreach(c => println(c.toString()))

    connectedCompIdsToVerticesAscii.take(10).foreach(c => println(c.toString()))

    val relationsAscii  = connectedCompIdsToVerticesAscii.map(
      x => (
        x._1,
        x._2.filter(c => c != x._1)
      )
    )

    relationsAscii.take(10).foreach(c => println(c.toString()))







    spark.stop()

  }
}
