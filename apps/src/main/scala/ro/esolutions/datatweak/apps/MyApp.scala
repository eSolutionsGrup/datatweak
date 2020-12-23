package ro.esolutions.datatweak.apps

import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import ro.esolutions.datatweak.apps.contexts.MyContext
import pureconfig._
import pureconfig.generic.auto._
import ro.esolutions.datatweak.SparkApp
import ro.esolutions.datatweak.implicits._
import ro.esolutions.datatweak.io._

object MyApp extends SparkApp[MyContext, Unit ]{
  override def createContext(conf: ConfigSource): MyContext = conf.loadOrThrow[MyContext]

  override def run(implicit spark: SparkSession, context: MyContext): Unit = {
    val df = spark.source(context.input).read
    df.sink(context.output).write
  }
}
