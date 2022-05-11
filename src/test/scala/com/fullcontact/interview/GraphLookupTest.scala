package com.fullcontact.interview

import com.fullcontact.interview.IdGenerator.hash
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers

class GraphLookupTest extends SparkTest with Matchers {

  test("Simple Graph Vertex Test") {
    val sc = sparkSession.sparkContext
    val simpleGraph = buildSimple()
    val gl: GraphLookup = new GraphLookup(simpleGraph)
    val b = vertexRDD("B")
    val c = vertexRDD("C")
    val a = vertexRDD("A")

    gl.getNeighbors(b).collect() shouldEqual Array("B: A C B")
    gl.getNeighbors(c).collect() shouldEqual Array("C: B C")
    gl.getNeighbors(a).collect() shouldEqual Array("A: B A")
  }

  test("Simple Graph Edge Test") {
    val sc = sparkSession.sparkContext
    val simpleGraph = buildSimple()
    val gl: GraphLookup = new GraphLookup(simpleGraph)
    val b = vertexRDD("B")
    val c = vertexRDD("C")
    val a = vertexRDD("A")

    gl.getEdges(a).collect() shouldEqual Array("A: A B")
    gl.getEdges(c).collect() shouldEqual Array("C: B C")
    gl.getEdges(b).collect() should contain theSameElementsAs Array( "B: A B", "B: B C")
  }

  test("Multiedge Graph Edge and Vertex Test") {
    val sc = sparkSession.sparkContext

    // contains multiple edges and connected component
    val lines = sc.parallelize(Array(
      "D E F",
      "D E",
      "F G"))
    // using graph builder here for the sake of brevity
    val graph = GraphBuilder.buildFrom(sc, lines)
    val gl: GraphLookup = new GraphLookup(graph)

    gl.getNeighbors(vertexRDD("D")).collect shouldEqual Array("D: E F D")
    gl.getNeighbors(vertexRDD("F")).collect shouldEqual Array("F: D E G F")

    gl.getEdges(vertexRDD("D")).collect should contain theSameElementsAs Array("D: D E F", "D: D E")
    gl.getEdges(vertexRDD("F")).collect should contain theSameElementsAs Array("F: D E F", "F: F G")
  }


  /**
    * Build simple test graph containing three vertices
    * A - B - C
    */
  def buildSimple(): Graph[String, String] ={
    val sc = sparkSession.sparkContext
    // Create an RDD for the vertices
    val simpleVertex: RDD[(VertexId, String)] = sc.parallelize(
      Array((hash("A"), "A"), (hash("B"), "B"), (hash("C"), "C"))
    )
    // Create an RDD for edges
    val simpleEdges: RDD[Edge[String]] = sc.parallelize(
      Array( Edge(hash("B"), hash("A"), "A B"),
        Edge(hash("B"), hash("C"), "B C"))
    )
    Graph(simpleVertex, simpleEdges)
  }

  def vertexRDD(s: String): VertexRDD[String] = VertexRDD(sparkSession.sparkContext.parallelize(Seq((hash(s), s))))

}
