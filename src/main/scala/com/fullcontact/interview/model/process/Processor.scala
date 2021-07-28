package com.fullcontact.interview.model.process

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

object Processor {

  def apply(recordsDf: Dataset[Row], queries: Dataset[Row]): (DataFrame, DataFrame) = {
    val joinedDf = recordsDf.join(broadcast(queries), trim(recordsDf.col("record_ids")).contains(trim(queries.col("query_id"))))
      .withColumn("record_ids", concat_ws(" ", array_remove(split(col("record_ids"), "\\s+"), col("query_id"))))
      .select("query_id", "record_ids")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val actualDf = joinedDf
      .withColumn("output_line", concat_ws(":", col("query_id"), col("record_ids")))
      .orderBy("query_id")
      .select("output_line")
      .orderBy("output_line")

    val mergedDf = joinedDf.groupBy("query_id").agg(collect_set(col("record_ids")).as("ids_list"))
      .withColumn("output_line",
        concat_ws(":", col("query_id"), concat_ws(" ", col("ids_list"))))
      .orderBy("query_id")
      .select("output_line")

    (actualDf, mergedDf)

  }

}
