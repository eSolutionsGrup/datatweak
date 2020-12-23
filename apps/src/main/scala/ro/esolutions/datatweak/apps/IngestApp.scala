package ro.esolutions.datatweak.apps

import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import ro.esolutions.datatweak.apps.contexts.IngestContext
import pureconfig._
import pureconfig.generic.auto._
import ro.esolutions.datatweak.SparkApp
import ro.esolutions.datatweak.implicits._
import ro.esolutions.datatweak.io._

object IngestApp extends SparkApp[IngestContext, Unit ]{
  override def createContext(conf: ConfigSource): IngestContext = conf.loadOrThrow[IngestContext]

  override def run(implicit spark: SparkSession, context: IngestContext): Unit = {
    val df = spark.source(context.input).read
    df.sink(context.output).write
  }
}
