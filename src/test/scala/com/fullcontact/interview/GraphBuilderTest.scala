package com.fullcontact.interview

import org.scalatest.Matchers
import com.fullcontact.interview.IdGenerator._
import org.apache.spark.graphx.{Edge, VertexRDD}
import org.apache.spark.rdd.RDD

class GraphBuilderTest extends SparkTest with Matchers {

  test("build simple graph") {
    val sc = sparkSession.sparkContext

    /*
        Small example
        A - B - C
     */
    val lines = sc.parallelize(Array(
      "A B",
      "B C"))
    val graph = GraphBuilder.buildFrom(sc, lines)

    val expectedVertices = VertexRDD(sc.parallelize(
      Array((hash("A"), "A"), (hash("B"), "B"), (hash("C"), "C"))
    ))

    val expectedEdges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(hash("A"), hash("B"), "A B"),
      Edge(hash("B"), hash("C"), "B C"))
    )

    graph.vertices.collect() should contain theSameElementsAs expectedVertices.collect()
    graph.edges.collect() should contain theSameElementsAs expectedEdges.collect()
  }

  test("build multiedge graph") {
    val sc = sparkSession.sparkContext

    /*
        Multiple edges and a fully connected component
        D = E
         \ /
          F - G     (ASCII art is not my strong suit)
     */
    val lines = sc.parallelize(Array(
      "D E F",
      "D E",
      "F G"
    ))
    val graph = GraphBuilder.buildFrom(sc, lines)

    val expectedVertices = VertexRDD(sc.parallelize(
      Array((hash("D"), "D"), (hash("E"), "E"), (hash("F"), "F"), (hash("G"), "G"))
    ))

    val expectedEdges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(hash("D"), hash("E"), "D E F"),
      Edge(hash("D"), hash("E"), "D E"),
      Edge(hash("E"), hash("F"), "D E F"),
      Edge(hash("D"), hash("F"), "D E F"),
      Edge(hash("F"), hash("G"), "F G"))
    )

    graph.vertices.collect() should contain theSameElementsAs expectedVertices.collect()
    graph.edges.collect() should contain theSameElementsAs expectedEdges.collect()
  }
}
