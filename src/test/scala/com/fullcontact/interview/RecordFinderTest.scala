package com.fullcontact.interview

import org.scalatest.{FunSuite, Matchers}

class RecordFinderTest extends FunSuite with Matchers {
  test("todo") {
    val rec = RecordFinder.main(args = Array(""))

    val numQueryItems = rec._2
    val numLongAsciiConvertItems = rec._3

    assert(numQueryItems == numLongAsciiConvertItems) // check integrity of converting ASCII Query/Record data to Long for GraphX

  }
}
