package com.fullcontact.interview

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

object RecordFinder {

  final val recordFilePath = "Records.txt"
  final val queriesFilePath = "Queries.txt"

  def main(args: Array[String]): Unit = {

    val outputPath = System.getProperty("user.dir")+"/output/"
    val directory = new Directory(new File(outputPath))

    val conf = new SparkConf().setAppName("Identity Resolution Problem by Teng Zhang").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    //Read records and queries text files and create RDDs
      val recordRdd = Reader.recordReader(recordFilePath,sparkContext)
      val queriesRdd = Reader.queriesReader(queriesFilePath, sparkContext)

      //Record and queries rdds are created in (key, value) structure for left outer join further down the process
      val recordTupleRdd = recordDataProcessor(recordRdd)
      val queriesTupleRdd = queriesRdd.map(x => (x, None))

    //transform rdds to output formats
      val transformedRdd = transformer(queriesTupleRdd, recordTupleRdd)
      val transformedUniqueKeysWithCombinedValuesRdd = transformByDistinctKey(transformedRdd)

      if(directory.exists){
        directory.deleteRecursively()
      }

      Writer.writeToFile(transformedRdd, outputPath+"Output1")
      Writer.writeToFile(transformedUniqueKeysWithCombinedValuesRdd, outputPath+"Output2")

  }
  private[interview] def transformer(queriesTupleRdd: RDD[(String, None.type)], recordTupleRdd: RDD[(String, Array[String])]) = {
    queriesTupleRdd.leftOuterJoin(recordTupleRdd).map{
      case(identityKey, (none, recordValues)) =>
        val formattedIdentityKey = identityKey.concat(":")
        recordValues match {

          case None => (formattedIdentityKey, Array[String]())
          case _ => (formattedIdentityKey, recordValues.get)
        }
    }.distinct()
  }

  private[interview] def transformByDistinctKey(transformedRdd: RDD[(String, Array[String])]) = {
    transformedRdd.groupBy(_._1).map{case(key, listsOfListOfValues) => {
      var list = collection.mutable.Set[String]()
      val listOfStr = listsOfListOfValues.toList
      for(i<-listOfStr.indices) {
        list = list++listOfStr(i)._2
      }
      (key, list.toArray)
    }}
  }

  private[interview] def recordDataProcessor(rdd: RDD[Array[String]]): RDD[(String, Array[String])] = {
    rdd.flatMap(recordRow=> {
      val list = new ListBuffer[(String, Array[String])]
      for(i <- recordRow.indices) {
        list += ((recordRow(i), recordRow))
      }
      list
    })
  }
}
