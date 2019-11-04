package com.fullcontact.interview

object RecordFinderArguments {

  case class RecordFinderConfig(
    recordsFile: String = "Records.txt",
    queriesFile: String = "Queries.txt",
    report1Dir: String = "Output1",
    report2Dir: String = "Output2")

  val parser = new scopt.OptionParser[RecordFinderConfig]("scopt") {
    opt[String]('r', "recordsFile").action ( (value,config) => config.copy
    (recordsFile = value)).text("HDFS Directory/file with Records of id relationships ")
    opt[String]('q', "queriesFile").action ( (value,config) => config.copy
    (queriesFile = value)).text("HDFS Directory/file with ids to query")
    opt[String]('o', "report1Dir").action ( (value,config) => config.copy
    (report1Dir = value)).text("HDFS Directory where the first report will be stored")
    opt[String]('a', "report2Dir").action ( (value,config) => config.copy
    (report2Dir = value)).text("HDFS Directory where the second report will be stored")
  }

  def parse(args: Array[String]): RecordFinderConfig =
    parser.parse(args, RecordFinderConfig()) match {
      case Some(config) => config
      case None => throw new RuntimeException("Arguments to RecordFinder are not valid")
    }
}
