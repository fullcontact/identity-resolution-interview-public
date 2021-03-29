package com.fullcontact.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode,split,lit,array,concat,concat_ws,row_number,collect_set,coalesce,col,when,array_contains}

import org.apache.spark.sql.expressions.Window

object RecordFinder {
  def main(args: Array[String]): Unit = {
    // TODO: Implement your job here

    val spark = SparkSession.builder.master("local[*]").appName("full_contact_home_assignment").getOrCreate()
    val sc = spark.sparkContext

    val sqlContext = spark.sqlContext
    import spark.implicits._
    var queries_path = args(0);
    var records_path = args(1);
    var output1_path = args(2);
    var output2_path = args(3);

   var queries_df =spark.read.text(queries_path).withColumnRenamed("value","queries_value")

    var records_df =spark.read.text(records_path).withColumnRenamed("value","records_value")

    val windowSpec = Window.orderBy("records_value")
    var records_ = records_df.withColumn("row",row_number().over(windowSpec)).withColumn("after_explode",explode(split($"records_value","\\s+"))).repartition($"row")

   var records_set = records_.groupBy($"row").agg(collect_set($"after_explode").as("records_list"))

    var records_agg = records_set.join(records_, records_("row") === records_set("row"))

    var combined_data = queries_df.join(records_agg,queries_df("queries_value") === records_agg("after_explode"),"left")

    var result1=combined_data.withColumn("output1",concat($"queries_value",lit(":"),when($"records_value".isNull,"None").otherwise(col("records_value"))))
    var r2=result1.where($"records_value".isNull).select("output1")
    var res = result1.select("output1")

    var result2 = result1.withColumn("deDup",explode($"records_list")).filter($"queries_value" !== $"deDup")


    var output2=result2.groupBy($"queries_value").agg(collect_set($"deDup").as("deDup_list")).select($"queries_value",concat_ws(" ",$"deDup_list").as("output2"))


    var out2=output2.withColumn("actual_result",concat($"queries_value",lit(":"),$"output2")).select("actual_result")

    var output_2 = out2.union(r2)
    res.coalesce(1).write.format("text").option("header","false").save(output1_path)
    res.coalesce(1).write.format("text").option("header","false").save(output2_path)
   spark.stop()
  }
}
