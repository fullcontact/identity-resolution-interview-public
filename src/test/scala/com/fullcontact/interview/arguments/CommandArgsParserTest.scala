package com.fullcontact.interview.arguments

import com.fullcontact.interview.RecordFinder.cmdConfigParser
//import org.junit.runner.RunWith
//import org.scalatest.FlatSpec
//import org.scalatest.junit.JUnitRunner

import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

class CommandArgsParserTest extends FunSuite with Matchers {

  private var cmdConfigParser: CommandArgsParser = new CommandArgsParser()

  test("Test config parser to take argument set, minus local mode"){

        val args: Array[String] = Array(
          "-r", "./Records.txt", "-q", "./Queries.txt", "-u", "Output1.txt", "-m", "Output2.txt")

        val configOption: Option[CommandArgs] = cmdConfigParser.parse(args.toSeq, CommandArgs())
        assertResult(true)(configOption.isDefined)
        val config = configOption.get
        assertResult("./Records.txt")(config.recordFilePath)
        assertResult("./Queries.txt")(config.queryFilePath)
        assertResult(true)(config.localMode)
        assertResult("Output1.txt")(config.unmergedOutputPath)
        assertResult("Output2.txt")(config.mergedOutputPath)
  }

    test ("Test config parser to take argument set, with local mode") {
      val args: Array[String] = Array(
        "-r", "./Records.txt", "-q", "./Queries.txt", "-u", "Output1.txt", "-m", "Output2.txt", "-l", "false")

      val configOption: Option[CommandArgs] = cmdConfigParser.parse(args.toSeq, CommandArgs())
      assertResult(true)(configOption.isDefined)
      val config = configOption.get
      assertResult("./Records.txt")(config.recordFilePath)
      assertResult("./Queries.txt")(config.queryFilePath)
      assertResult(false)(config.localMode)
      assertResult("Output1.txt")(config.unmergedOutputPath)
      assertResult("Output2.txt")(config.mergedOutputPath)

    }
}