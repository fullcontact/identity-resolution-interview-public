package com.fullcontact.interview.arguments

import scopt._

/**
 * Parser to parse the command line arguments.
 *
 * @author Robert de Lorimier
 */
class CommandArgsParser extends OptionParser[CommandArgs]("""See Readme for usage""".stripMargin) {
  override def errorOnUnknownArgument = false

  // -l, --localMode
  opt[Boolean]('l', "localMode")
    .action((x, c) => c.copy(localMode = x))
    .text("Whether application will run in local mode")

  // -r, --recordFilePath
  opt[String]('r', "recordFilePath")
    .required()
    .action((x, c) => c.copy(recordFilePath = x))
    .text("Path to record file")

  // -q, --queryFilePath
  opt[String]('q', "queryFilePath")
    .required()
    .action((x, c) => c.copy(queryFilePath = x))
    .text("Path to query file")

  // -u, --unmergedOutputPath
  opt[String]('u', "unmergedOutputPath")
    .required()
    .action((x, c) => c.copy(unmergedOutputPath = x))
    .text("Target index of elasticsearch")

  // -u, --unmergedOutputPath
  opt[String]('m', "mergedOutputPath")
    .required()
    .action((x, c) => c.copy(mergedOutputPath = x))
    .text("Target index of elasticsearch")

}


