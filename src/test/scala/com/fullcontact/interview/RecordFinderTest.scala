package com.fullcontact.interview


import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test")
      .getOrCreate()
  }

}


class DatasetSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  it("aliases a DataFrame") {
//    System.setProperty("hadoop.home.dir", "C:\\Users\\Ashwin.Kumar\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")

    val records = Seq(
      "jjjj aaaa kkkk",
      "lili ffff aaaa",
      "lulu ddd",
      "ddd uuu"
    ).toDS()

    val query = Seq(
      "aaaa",
      "ffff",
      "ddd",
      "ddd"
    ).toDS()

    val actualDF = RecordFinder.getIdentityMatch(spark,records,query).toDF()

    actualDF.show(false)

    val expectedDF = Seq(
      "ddd:ddd uuu lulu",
      "aaaa:lili ffff aaaa jjjj kkkk",
        "ffff:lili ffff aaaa"
    ).toDF()

    assertSmallDatasetEquality(actualDF, expectedDF,orderedComparison = false)

  }
}



