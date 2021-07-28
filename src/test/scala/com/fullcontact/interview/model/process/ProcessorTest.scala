package com.fullcontact.interview.model.process

import com.fullcontact.interview.SparkSessionProvider
import com.fullcontact.interview.util.Helper.{createDataframeFromList, getDataAsString}
import org.scalatest.{FunSpec, Matchers}


class ProcessorTest extends FunSpec with Matchers with SparkSessionProvider {
  describe("Verify the matched ids") {

    //Given
    val queriesDf = createDataframeFromList(Seq("KDYBNMV",
      "SSATKEJ",
      "GACTECQ",
      "VYKNWWE",
      "GKYIRQQ"), "query_id")
    val recordsIdsDf = createDataframeFromList(Seq("EKTEUFL QZTWYBW GKYIRQQ AOKGXXJ MEUEDQW",
      "WNOSVHQ SSATKEJ KROZJRA GTXDPZL GACTECQ",
      "CUVKNHZ PSULGHG GKYIRQQ XSBXECH",
      "XAFESMP LPBUEVJ ZCXUHWM LGVSSTL GKYIRQQ"), "record_ids")

    val expectedActualData = List(List("GACTECQ:WNOSVHQ SSATKEJ KROZJRA GTXDPZL"),
      List("GKYIRQQ:CUVKNHZ PSULGHG XSBXECH"),
      List("GKYIRQQ:EKTEUFL QZTWYBW AOKGXXJ MEUEDQW"),
      List("GKYIRQQ:XAFESMP LPBUEVJ ZCXUHWM LGVSSTL"),
      List("SSATKEJ:WNOSVHQ KROZJRA GTXDPZL GACTECQ"))

    val expectedMergedData = List(List("GACTECQ:WNOSVHQ SSATKEJ KROZJRA GTXDPZL"),
      List("GKYIRQQ:EKTEUFL QZTWYBW AOKGXXJ MEUEDQW XAFESMP LPBUEVJ ZCXUHWM LGVSSTL CUVKNHZ PSULGHG XSBXECH"),
      List("SSATKEJ:WNOSVHQ KROZJRA GTXDPZL GACTECQ"))
    //When
    val (actualDf, mergedDf) = Processor(recordsIdsDf, queriesDf)
    //Then
    println(getDataAsString(actualDf))
    println(getDataAsString(mergedDf))
    getDataAsString(actualDf) should contain theSameElementsAs expectedActualData
    getDataAsString(mergedDf) should contain theSameElementsAs expectedMergedData
  }
}
