package com.fullcontact.interview
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Helper {

  def createPairs(ref: String, neighbors: Array[String]):Array[Array[String]]  = {
    neighbors.flatMap(x => Array(Array(ref, x), Array(x, ref)))
  }


  def asciiToLong(x: String) = {
    x.map(_.toInt.toString).mkString("").toDouble.toLong
  }

  def getNeighbors(items: Array[String]) = {
    val num = items.length
    items.takeRight(num-1)
  }

  def getMatchesAscii(m: Long, longToQueryAscii:RDD[(Long, String)]) = {

    val asciiValue = longToQueryAscii.filter(x => x._1 == m).map(_._2)
    longToQueryAscii.take(10).foreach(v => v._1.toString)

    asciiValue

  }





}
