package ro.esolutions.spark.io.sources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import ro.esolutions.spark.io.sources.SourceConfiguration.JdbcSourceConfiguration
import ro.esolutions.spark.io._

import scala.util.{Failure, Success, Try}

final case class JdbcDataSource(configuration: JdbcSourceConfiguration)
        extends DataSource[JdbcSourceConfiguration] with Logging {

  override def read(implicit spark: SparkSession): DataFrame = {
    logInfo(s"Reading data as '${configuration.format}' from the '${configuration.table}' " +
      s"table of '${configuration.url}'.")
    Try(createReader(configuration).load()) match {
      case Success(df) =>
        logInfo(s"Successfully read the data as '${configuration.format}' from " +
          s"the '${configuration.table}' table of '${configuration.url}'")

        df
      case Failure(e) =>
        val message = s"Failed to read the data as '${configuration.format}' from " +
          s"the '${configuration.table}' table of '${configuration.url}' (Full configuration: ${configuration})"
        logError(message, e)
        throw new Exception(message, e)
    }
  }

  private def createReader(configuration: JdbcSourceConfiguration)(implicit spark: SparkSession): DataFrameReader = {
    spark.read
      .format(configuration.format.toString)
      .options(configuration.readerOptions)
  }
}
