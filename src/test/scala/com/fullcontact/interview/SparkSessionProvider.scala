package com.fullcontact.interview

import com.fullcontact.interview.util.TestSparkInit
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

trait SparkSessionProvider extends Suite {

  implicit lazy val sparkSession: SparkSession =
    setExecutionContextParameters("512m", "1024m", 2, 2, 4)(new TestSparkInit().createSession())

  private def setExecutionContextParameters(driverMemory: String,
                                            executorMemory: String,
                                            executorCores: Int, driverCores: Int,
                                            executorInstances: Int)(implicit sparkSession: SparkSession): SparkSession = {
    sparkSession.conf.set("spark.driver.memory", driverMemory)
    sparkSession.conf.set("spark.executor.memory", executorMemory)
    sparkSession.conf.set("spark.executor.cores", driverCores)
    sparkSession.conf.set("spark.driver.cores", executorCores)
    sparkSession.conf.set("spark.executor.instances", executorInstances)
    sparkSession
  }
}
