package ro.esolutions.datatweak.apps

import org.apache.spark.sql.{DataFrame, SparkSession}
import ro.esolutions.datatweak.io.sinks.SinkConfiguration
import ro.esolutions.datatweak.io.sources.SourceConfiguration
import ro.esolutions.datatweak.io._
import ro.esolutions.datatweak.implicits._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

package object contexts {
  case class InputSource(view: String, input: SourceConfiguration) {
    def load(implicit spark: SparkSession): Unit = {
      spark.source(input).read.createOrReplaceTempView(view)
    }
  }
  case class OutputSink(source: String, output: SinkConfiguration) {
    def write(implicit spark: SparkSession): Future[DataFrame] = {
      Future {
        spark.table(source).sink(output).write
      }
    }
  }
  case class Step(view: String, query: String) {
    def exec(implicit spark: SparkSession): Unit = {
      spark.sql(query).createOrReplaceTempView(view)
    }
  }
  case class QueryContext(source: List[InputSource], output: SinkConfiguration, query: String)
  case class IngestContext(input: SourceConfiguration, output: SinkConfiguration)
  case class StepsContext(source: List[InputSource], steps: List[Step], sink: List[OutputSink])
}
