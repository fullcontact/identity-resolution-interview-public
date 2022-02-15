package com.fullcontact.interview.arguments

case class CommandArgs(
                        recordFilePath: String = "",
                        queryFilePath: String = "",
                        unmergedOutputPath: String = "",
                        mergedOutputPath: String = "",
                        localMode: Boolean = true
                      )
