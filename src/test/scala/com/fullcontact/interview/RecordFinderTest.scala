/*
Author: FullContact
Modified By: Jeff Willingham
Creation Date: unknown
Modification Date: 2020-07-28
Purpose: This script checks Queries.txt, Records.txt and RecordFinder.scala to determine if they (1) exist and (2) contain data.
*/

package com.fullcontact.interview

import org.scalatest.{FunSuite, Matchers}
import java.io.File

class RecordFinderTest extends FunSuite with Matchers {
  test("file checks") {
    val inputFileOne = new File("Queries.txt")
    inputFileOne should exist
    inputFileOne.length should not be 0
    val inputFileTwo = new File("Records.txt")
    inputFileTwo should exist
    inputFileTwo.length should not be 0
    val mainScript = new File("src/main/scala/com/fullcontact/interview/RecordFinder.scala")
    mainScript should exist
    mainScript.length should not be 0
  }
}