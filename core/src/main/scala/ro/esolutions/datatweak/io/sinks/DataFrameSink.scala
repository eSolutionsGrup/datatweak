package ro.esolutions.datatweak.io.sinks

import org.apache.spark.sql.DataFrame
import ro.esolutions.datatweak.io.sinks.SinkConfiguration._
import ro.esolutions.datatweak.io.{DataSink, DataSinkConfiguration}

trait DataFrameSink[Config <: DataSinkConfiguration, WriteOut] {
  def data: DataFrame
  def sink: DataSink[Config, WriteOut]
  def write: WriteOut = sink.write(data)
}

object DataFrameSink {
  case class FileDataFrameSink(configuration: FileSinkConfiguration, data: DataFrame)
          extends DataFrameSink[FileSinkConfiguration, DataFrame] {
    override def sink: DataSink[FileSinkConfiguration, DataFrame] = FileDataSink(configuration)
  }
  case class JdbcDataFrameSink(configuration: JdbcSinkConfiguration, data: DataFrame)
          extends DataFrameSink[JdbcSinkConfiguration, DataFrame] {
    override def sink: DataSink[JdbcSinkConfiguration, DataFrame] = JdbcDataSink(configuration)
  }
}
