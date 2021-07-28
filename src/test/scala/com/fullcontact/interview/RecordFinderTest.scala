package com.fullcontact.interview

import org.scalatest.{FunSuite, Matchers}
import com.fullcontact.interview.RecordFinder

class RecordFinderTest extends FunSuite with Matchers {
  test("Proper validation of an ID having/not having 7 uppercase letters") {
    assert(RecordFinder.isWordNot7Uppers("ABCDEFG") == 0)
    assert(RecordFinder.isWordNot7Uppers("ABCDEFg") == 1)
    assert(RecordFinder.isWordNot7Uppers("ABCD FG") == 1)
    assert(RecordFinder.isWordNot7Uppers("1BCDEFG") == 1)
  }
}
