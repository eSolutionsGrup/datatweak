package ro.esolutions.datatweak.io.sources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import ro.esolutions.datatweak.io.sources.SourceConfiguration.FileSourceConfiguration
import ro.esolutions.datatweak.io._

import scala.util.{Failure, Success, Try}

final case class FileDataSource(configuration: FileSourceConfiguration)
        extends DataSource[FileSourceConfiguration] with Logging {

  override def read(implicit spark: SparkSession): DataFrame = {
    logInfo(s"Reading data as '${configuration.format}' from '${configuration.path}'.")

    Try(createReader(configuration).load(configuration.path)) match {
      case Success(df) =>
        logInfo(s"Successfully read the data as '${configuration.format}' from '${configuration.path}'")
        df
      case Failure(e) =>
        val message = s"Failed to read the data as '${configuration.format}' from '${configuration.path}'"
        logError(message, e)
        throw DataSourceException(message, e)
    }
  }

  private def createReader(configuration: FileSourceConfiguration)(implicit spark: SparkSession): DataFrameReader = {
    val format = configuration.format.toString
    val basicReader = spark.read
      .format(format)
      .options(configuration.options)

    configuration.schema match {
      case Some(schema) =>
        logDebug(s"Initializing the '$format' DataFrame loader using the specified schema.")
        basicReader.schema(schema)
      case None =>
        logDebug(s"Initializing the '$format' DataFrame loader inferring the schema.")
        basicReader
    }
  }

}
