package com.fullcontact.interview

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import IdGenerator.hash

object GraphBuilder {

  private final val WHITESPACE : String = "\\s+"

  def buildFromFile(sc : SparkContext, fileName : String) : Graph[String, String] = {
    val lines = sc.textFile(fileName)
    buildFrom(sc, lines)
  }

  def buildFrom(sc: SparkContext, lines: RDD[String]): Graph[String, String] = {
    // build vertices: each id present in any record
    val vertices = lines.flatMap(_.split(WHITESPACE)).distinct.map(id => (hash(id), id))

    // build edges: link ids by the record(s) associating both
    val edges = lines.flatMap { line =>
      // each id on this line will have an association to each other id
      val tokens = line.split(WHITESPACE).toSeq

      // build an edge for each pair combination of ids in record.
      // Direction ultimately won't matter as we can search for adjacency ignoring direction
      tokens.combinations(2).map(pair => {
        Edge(hash(pair(0)), hash(pair(1)), line)
      })
    }
    Graph(vertices, edges)
  }
}
