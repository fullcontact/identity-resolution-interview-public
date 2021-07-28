package com.fullcontact.interview.model.write

import org.apache.spark.sql.{DataFrame, SaveMode}

object Writer {

  def textWriter(processedDf: DataFrame, outputPath: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    processedDf.write.mode(saveMode).text(outputPath)
  }

}
