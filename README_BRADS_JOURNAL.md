# Brad's FullContact Homework Journal

This is a kind of stream-of-consciousness journal I'm (bradleypmartin is) keeping while working through the FullContact homework project.

I wouldn't expect people to read through this in detail at all, but I thought I'd include it because it gives kind of a view into my process for working this type of problem (and could remind me of points to talk about in the in-person or by-video-chat review phase if my Scala doesn't turn people away :) ).

## 2021-07-27 10:29 am - First Thoughts

* So...this project is really neat! I like how well it simplifies FC's technical backend while fitting into a few hours of bandwidth for candidates.

* We'll essentially be building:
  * For Output1, a mapping of ID to rows-of-`Records.txt`
  * For Output2, pretty much a straight-up encoded immediate neighbor graph/matrix

* Testing and preliminary coding will probably involve:
  * Making sure the inputs (both records and queries) have _entirely_ 7-char strings from ASCII 65-90
  * Gracefully handling the case when we _don't_ encounter that
  * Looking for the breadth of our 'key space' (`Records.txt` and `Queries.txt` have about 50k lines and 100k lines each. Given that and the 7-letter structure of our IDs, what types of encodings would be most advantageous here?)
  * Figuring out how much memory we'll be spending with each intermediate data structure we'll be using for both outputs here (are we going to be pushing up against any limits of modern local hardware?).

* I'll start out by simply parsing these text files into some helpful data structures and doing light analysis of what we've got.

## 2021-07-27 11:09 am - Running Spark locally to ingest txt files

* I'd like to run Spark locally to ingest the records/queries files
* I'll do some analysis after that
* The following worked just fine to ingest `Records.txt` and print off a few lines:

```
package com.fullcontact.interview
import org.apache.spark.{SparkConf, SparkContext}

object RecordFinder {
  def main(args: Array[String]): Unit = {
    // Spark setup/init
    val conf = new SparkConf()
      .setAppName("BradsRecordFinder")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Reading info from Records.txt file
    val recordsRawRDD = sc.textFile("Records.txt")
    println("Records RDD sample:")
    recordsRawRDD.take(10).foreach(println)
  }
}
```

* Committing the above before doing some validation on the `Records.txt` data








































##
