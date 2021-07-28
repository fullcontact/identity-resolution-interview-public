package com.fullcontact.interview.model.read

import com.fullcontact.interview.SparkSessionProvider
import com.fullcontact.interview.util.Helper._
import org.scalatest.{FunSpec, Matchers}


class ReaderTest extends FunSpec with Matchers with SparkSessionProvider {
  describe("Verify the read function should return valid data") {

    //Given
    val queriesPath = findResourcePath("Queries.txt")
    val RecordsPath = findResourcePath("Records.txt")

    val expectedQueriesData = List("KDYBNMV",
      "SSATKEJ",
      "GACTECQ",
      "VYKNWWE",
      "GKYIRQQ")

    val expectedRecordsData = List("EKTEUFL QZTWYBW GKYIRQQ AOKGXXJ MEUEDQW",
      "WNOSVHQ SSATKEJ KROZJRA GTXDPZL GACTECQ",
      "CUVKNHZ PSULGHG GKYIRQQ XSBXECH",
      "XAFESMP LPBUEVJ ZCXUHWM LGVSSTL GKYIRQQ")

    //When
    val queriesIdsDf = Reader.textReader(queriesPath).toDF()
    val RecordIdsDf = Reader.textReader(RecordsPath).toDF()
    //Then
    getDataAsString(queriesIdsDf).flatten should contain theSameElementsAs expectedQueriesData
    getDataAsString(RecordIdsDf).flatten should contain theSameElementsAs expectedRecordsData
  }
}
