package ro.esolutions.datatweak

import org.apache.spark.internal.Logging
import pureconfig.ConfigSource

import scala.util.{Failure, Success, Try}

trait SparkApp[Context, Result] extends JobRunnable[Context, Result] with Logging {

  def createContext(conf: ConfigSource): Context

  def main(args: Array[String]): Unit = {
    implicit val (config, spark) = ConfigAndSession(args)

    val output = for {
      context <- Try(createContext(config))
      result <- Try(run(spark, context))
    } yield result

    output match {
      case Success(_) => log.info(s"${spark.sparkContext.appName} successfully run")
      case Failure(e) => log.error(s"${spark.sparkContext.appName} failed", e)
    }

    Try(spark.close) match {
      case Success(_) => log.info(s"${spark.sparkContext.appName}: Spark session closed.")
      case Failure(e) => log.error(s"${spark.sparkContext.appName}: Failed to close the spark session.", e)
    }

    output.get
  }

}
