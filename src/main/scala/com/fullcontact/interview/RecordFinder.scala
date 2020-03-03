package com.fullcontact.interview

import org.apache.spark.SparkConf

import scala.reflect.io.Directory
import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object RecordFinder {

  def main(args: Array[String]): Unit = {
    if(args.size != 2)
    {
      println("Need 2 arguments <Queries_File> <Records_File>")
      System.exit(1)
    }

     val sparkConf = new SparkConf()
        .set("spark.hadoop.validateOutputSpecs", "false") //bit redundant with deleting dirs

      val sparkSession = SparkSession.builder()
        .master("local[*]")
        .config(sparkConf)
        .getOrCreate()

      val output1 = "./output1.txt"
      val output2 = "./output2.txt"

      //Delete recursively old output files.
      new Directory(new File(output1)).deleteRecursively()
      new Directory(new File(output2)).deleteRecursively()


    resolveIdentifiers(
        sparkSession,
        args(0),
        args(1),
        output1,
        output2
      )
  }


  /**
   *
   * @param sparkSession
   * @param queryData
   * @param recordData
   * @param output1
   * @param output2
   */
  def resolveIdentifiers(sparkSession: SparkSession,
                         queryData: String,
                         recordData: String,
                         output1: String,
                         output2: String): Unit = {

    val (joined_RDD, dedupe_RDD) = findResolvedRDDs(sparkSession,queryData,recordData)

    joined_RDD.values
      .coalesce(1)
      .saveAsTextFile(output1)

    dedupe_RDD.values
      .coalesce(1)
      .saveAsTextFile(output2)
  }


  /**
   *
   * @param sparkSession
   * @param queryPath
   * @param recordPath
   * @return
   */
    def findResolvedRDDs(sparkSession: SparkSession,queryPath: String,
                       recordPath: String): (RDD[(String, String)], RDD[(String, String)]) = {
    val sparkContext = sparkSession.sparkContext
    val queriesRdd: RDD[String] = sparkContext.textFile(queryPath)
    val recordsRdd: RDD[String] = sparkContext.textFile(recordPath)
    val newQueriesRdd = queriesRdd.filter{
      x=>x.size==7 && x==x.toUpperCase

    }map{
      x=>(x,"")
    }

    /*
      Here inverted_indexed_RDD is generated as an inverted index of all valid identifiers to its row value in file.
      inverted index is a pair RDD with (identifier, line) tuple
    */

    val inverted_indexed_RDD = recordsRdd.flatMap{
      case (line) =>
        line
          .split("""\W+""").filter{
          x=>x.size==7 && x==x.toUpperCase
        }.map {
          identifier => (identifier, line)
        }
    }

      /*
       Here we right outer join to queries rdd which is also a pair rdd of (identifier,"") to inverted Index created above on identifiers as key
       Here we right outer joined on identifiers to pick all queries even if they don't exist in record file.
       Since here queried RDD can have same identifier being queried multiple times, we can expect duplicates.
     */
      val duplicate_queries_joined_rdd = inverted_indexed_RDD.rightOuterJoin(newQueriesRdd).map{
      x =>{
        val value = x._2._1 match {
          case None    => "NA"
          case Some(s) => s
        }
        if(value!="NA"){
          (x._1,x._1 + " : " + value)
        }else{
          (x._1,x._1 + " : ")
        }
      }
    }

      /*
      Here we right outer join to queries rdd which is also a pair rdd of (identifier,"") to inverted Index created above on identifiers as key
      Here before performing join of inverted_index RDD, it was reduced to merge all same identifiers as one (k,v) pair and with distinct values.
      Again with rightouter join with queriesRDD to find all associated unique identifiers to queries identifiers
      Here we  used rightouterjoin to pick all queries even if they don't exist in record file.
      Since here queried RDD can't have duplicates, we should only see unique identifiers.
*/

    val distinct_query_rdd = newQueriesRdd.distinct()
    val identifier_to_unique_ids = inverted_indexed_RDD.map{
      case(x,y) => (x, y.split("""\W+""").filter{
        x=>x.size==7 && x==x.toUpperCase
      })
    }.reduceByKey{
      case (n1, n2) => (n1++n2).distinct
    }.map{
      x=>(x._1,x._2.distinct.mkString(" "))
    }.rightOuterJoin(distinct_query_rdd).map{
      x =>{
        val value = x._2._1 match {
          case None    => "NA"
          case Some(s) => s
        }
        if(value!="NA"){
          (x._1,x._1 + " : " + value)
        }else{
          (x._1,x._1 + " : ")
        }
      }    }

    (duplicate_queries_joined_rdd, identifier_to_unique_ids)
  }


}
