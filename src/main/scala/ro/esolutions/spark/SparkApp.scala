package ro.esolutions.spark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource

import scala.util.{Failure, Success, Try}

trait SparkApp[Context, Result] extends JobRunnable[Context, Result] with ConfigBuilder with Logging {

  def createContext(conf: ConfigSource): Context

  def main(args: Array[String]): Unit = {
    val appArgs = AppArgs(args)

    implicit val spark = createSparkSession(appArgs.appName)
    implicit val conf = appConfiguration(appArgs)

    val output = for {
      context <- Try(createContext(conf))
      result <- Try(run(spark, context))
    } yield result

    output match {
      case Success(_) => log.info(s"${appArgs.appName} successfully run")
      case Failure(e) => log.error(s"${appArgs.appName} failed", e)
    }

    Try(spark.close) match {
      case Success(_) => log.info(s"${appArgs.appName}: Spark session closed.")
      case Failure(e) => log.error(s"${appArgs.appName}: Failed to close the spark session.", e)
    }

    output.get
  }

  protected def createSparkSession(appName: String): SparkSession = {
    val sparkConfig = new SparkConf()
    val confSpark = sparkConfig.setAppName(appName)
      .setMaster(sparkConfig.get("spark.master", "local[*]"))

    SparkSession.builder().config(confSpark).getOrCreate()
  }

}
