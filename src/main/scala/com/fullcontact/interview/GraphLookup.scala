package com.fullcontact.interview

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

class GraphLookup(graph: Graph[String, String]) {

  val neighbors: VertexRDD[Array[(VertexId, String)]] = graph.collectNeighbors(EdgeDirection.Either)
  val edges: VertexRDD[Array[Edge[String]]] = graph.collectEdges(EdgeDirection.Either)
  neighbors.cache()
  edges.cache()

  /**
    * Look up adjacent vertices for each string in query rdd and create the following format string for each
    * SrcId: Neighbor1 Neighbor2 (...)
    * Resulting RDD should have only one line of output per line of input. Should be the deduplicated union
    * set of ids from matching records.
    * @param queryVertices RDD of id strings to look up, one per line
    * @return RDD of formatted strings, set of neighboring ids
    */
  def getNeighbors(queryVertices: VertexRDD[String]): RDD[String] = {
    queryVertices.innerJoin(neighbors){
      (id, vertexStr, neighbors) =>
        var neighborIds = neighbors.map{ case (_, strId) => strId}
        neighborIds = neighborIds.distinct :+ vertexStr // need ALL values from union of matching records, including self
        vertexStr+": "+neighborIds.mkString(" ")
    }.map {
      case (_, resultStr) => resultStr
    }
  }


  /**
    * Get edges (Full Record String) for each vertex in the input RDD. Each unique edge should correspond
    * with one line of output in the format
    * SrcId: RECORD
    * @param queryVertices RDD of id strings to look up, one per line
    * @return RDD of formatted records, one record per line
    */
  def getEdges(queryVertices: VertexRDD[String]): RDD[String] = {
    queryVertices.innerJoin(edges){
      (id, vertexStr, edges) =>
        val records = edges.map(_.attr).distinct
        records.map(r => vertexStr+": "+r)
    }.flatMap {
      case (_, resultStr) => resultStr
    }
  }
}
