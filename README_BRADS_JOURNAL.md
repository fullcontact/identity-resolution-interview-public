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

## 2021-07-27 11:39 am - Records and Queries validation

* Validations/tests to do next:
  * [x] 1 - Make sure both `Records.txt` and `Queries.txt` have _only_ 7-letter (uppercase)
  * [x] 2 - Find out how many distinct septuplets we have in `Records.txt` and `Queries.txt`.

* Findings:
  * Inspecting the files, `Records.txt` is about 1.6 MB in size; `Queries.txt` is about 800 KB in size.
  * If there are about 100k queries and somewhere around 50k records, that means we've got _on average_ 4 IDs per row of records, and each ID is taking up about 7-ish bytes of space on disk (which makes sense, I guess, for 8-bit ASCII characters)
  * I get 52772 rows in `Records.txt`.
  * I get 103279 rows in `Queries.txt`.
  * After a bit of work, I find that I've imported _no_ non-7-uppercase-letter words from either file.

## 2021-07-27 2:47 pm - Indexing Records; Mapping records to (ID String, Set[Int]) map

* I'd like to transform my Records RDD now to a Map of Strings keys (IDs) to _Set[Int]_ values pointing to the _index_ of the record in the Records RDD
  * This might seem kind of weird, but it'll give us a sort of 'just in time'/'lazy' way to output `Output1.txt` queries instead of redundantly repeating _every_ record row for _every_ ID that contains it.
  * It'll make our `Output1.txt` queries _somewhat_ slower while saving us having to store a bunch of redundant/non-normalized data.

* _UPDATE FROM BELOW_: It's actually a little easier (for at least `Output1.txt`) to simply get the ID:(records index) mapping flattened, then join against the queries on flattened.ID = queries.ID.

## 2021-07-27 3:56 pm - Flattening out the records

* I've flattened out the records to (record index : ID) 'pairs'; now will have to reassemble into a {ID : [record _indices_]} map.

## 2021-07-28 10:23 am - time to join up queries and ID:records index

* Time for my first join! I'll need to do the following:
  * Join the queries RDD to the recordsIndicesFlatDF on `queriesDF.ID` = `recordsIndicesFlatDF.ID`
  * Join the result of the above on `recordsIndicesFlatDF.recordsIndex` = `recordsSplitIndexedDF.recordsIndex`
  * Select `queriesDF.ID` and `recordsSplitIndexedDF.neighborArray` _and_ `queriesDF.queryIndex` _but not do any ordering or transforms just yet (thinking ahead to Output2 :) )

## 2021-07-28 11:13 am - Reviewing Output1 join results and getting ready to transform into text

So here's the kind of thing I see from my join output:

```
+-------+----------+--------------------+
|     ID|queryIndex|partialNeighborArray|
+-------+----------+--------------------+
|MIBEQWC|      2977|[ODLVKBD, YJQPIDO...|
|LGDPBFS|     18334|  [GTLLOMC, LGDPBFS]|
|RSCSFYV|     17542|[RSCSFYV, CUGOPLE...|
|DGYMWSH|     40066|  [DGYMWSH, UCONIWZ]|
|VKWWQBM|     33245|[GJKJEKK, VJBRCTI...|
|CYAZJIP|     71158|[LVDSTNS, CYAZJIP...|
|TRVKNRO|     68351|[XDBLOUQ, MXUTKEI...|
|LFIYDGA|     13289|[YMWYXPH, LFIYDGA...|
|MSJJROI|     35625|  [VYPXMGD, MSJJROI]|
|JLDNUWE|     25024|[HNZXKEX, THWTEUK...|
+-------+----------+--------------------+
```

I'm _just_ about ready to output my results to `Output1.txt`. Will need to make rows of the above like:

```
MIBEQWC: ODLVKBD YJQPIDO ...
LGDPBFS: GTLLOMC LGDPBFS ...
```

I'll do a little searching here to figure out how I want to do that. [This StackOverflow link](https://stackoverflow.com/questions/44537889/write-store-dataframe-in-text-file) has got pretty much just what I need.

## 2021-07-28 12:59 pm - Done with Output1 and on to Output2

So we've actually got _most_ of what we need done for Output 2 (believe it or not!) What we _really_ need to do now is take our joined pre-transform output from Output 1 and `ReduceByKey` (after transforming the `Array[String]` column to `Set[String]`).

## 2021-07-28 2:35 pm - Got Output2 done!

Output 2 is done; time to clean up and write some tests!

## 2021-07-28 7:03 pm - Done with tests; writing instructions/discussion

Time to wrap up and open the PR 'for real'.
