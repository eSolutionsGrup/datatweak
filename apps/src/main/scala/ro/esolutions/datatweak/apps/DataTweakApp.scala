package ro.esolutions.datatweak.apps

import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import ro.esolutions.datatweak.SparkApp
import ro.esolutions.datatweak.apps.contexts._
import ro.esolutions.datatweak.io._
import ro.esolutions.datatweak.implicits._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object DataTweakApp extends SparkApp[StepsContext, Unit] {
  override def createContext(conf: ConfigSource): StepsContext = conf.loadOrThrow[StepsContext]

  override def run(implicit spark: SparkSession, context: StepsContext): Unit = {
    context.source.foreach(_.load)
    context.steps.foreach(_.exec)
    context.sink.map(_.write).foreach(df => Await.ready(df, Duration.Inf))
  }
}
