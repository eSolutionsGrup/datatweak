package ro.esolutions.datatweak.io.sinks

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import ro.esolutions.datatweak.io.DataSink
import ro.esolutions.datatweak.io.sinks.SinkConfiguration.JdbcSinkConfiguration

import scala.util.{Failure, Success, Try}

case class JdbcDataSink(configuration: JdbcSinkConfiguration)
          extends DataSink[JdbcSinkConfiguration, DataFrame] with Logging {

  override def write(data: DataFrame): DataFrame = {
    logInfo(s"Writing data as '${configuration.format}' to the '${configuration.table}' " +
      s"table of '${configuration.url}'.")
    Try(configureWriter(data, configuration).save()) match {
      case Success(_) =>
        logInfo(s"Successfully saved the data as '${configuration.format}' " +
          s"to the '${configuration.table}' table of '${configuration.url}' " +
          s"(Full configuration: ${configuration}).")
        data
      case Failure(ex) =>
        val message = s"Failed to save the data as '${configuration.format}' " +
          s"to the '${configuration.table}' table of '${configuration.url}' " +
          s"(Full configuration: ${configuration})."
        logError(message)
        throw new DataSinkException(message, ex)
    }
  }

  private def configureWriter(data: DataFrame, configuration: JdbcSinkConfiguration): DataFrameWriter[Row] = {
    data.write.format(configuration.format.toString).mode(configuration.saveMode).options(configuration.writerOptions)
  }

}
