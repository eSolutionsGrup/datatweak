package ro.esolutions.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource

object ConfigAndSession extends ConfigBuilder {
  def apply(cmdlineArgs: Array[String]): (ConfigSource, SparkSession) = {
    Args(cmdlineArgs) match {
      case Some(args) => (createConfiguration(args), createSparkSession(args.appName))
      case None => throw new IllegalArgumentException("Failed to load specific args")
    }
  }

  private def createSparkSession(appName: String): SparkSession = {
    val sparkConfig = new SparkConf()
    val confSpark = sparkConfig.setAppName(appName)
      .setMaster(sparkConfig.get("spark.master", "local[*]"))

    SparkSession.builder().config(confSpark).getOrCreate()
  }

}
