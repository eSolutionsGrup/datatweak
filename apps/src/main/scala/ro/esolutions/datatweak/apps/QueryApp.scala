package ro.esolutions.datatweak.apps

import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import ro.esolutions.datatweak.SparkApp
import ro.esolutions.datatweak.implicits._
import ro.esolutions.datatweak.io._
import ro.esolutions.datatweak.apps.contexts.QueryContext

object QueryApp extends SparkApp[QueryContext, Unit] {
  override def createContext(conf: ConfigSource): QueryContext = conf.loadOrThrow[QueryContext]

  override def run(implicit spark: SparkSession, context: QueryContext): Unit = {
    context.source.foreach(s => spark.source(s.input).read.createOrReplaceTempView(s.view))
    val df = spark.sql(context.query)
    df.sink(context.output).write
  }
}
