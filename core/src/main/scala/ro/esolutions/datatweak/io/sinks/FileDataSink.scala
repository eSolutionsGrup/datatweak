package ro.esolutions.datatweak.io.sinks

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import ro.esolutions.datatweak.io.DataSink
import ro.esolutions.datatweak.io.sinks.SinkConfiguration.FileSinkConfiguration

import scala.util.{Failure, Success, Try}

final case class FileDataSink(configuration: FileSinkConfiguration)
          extends DataSink[FileSinkConfiguration, DataFrame] with Logging {

  override def write(data: DataFrame): DataFrame = {
    val writer = createWriter(data, configuration)
    writing(writer, configuration) match {
      case Success(_) =>
        logInfo(s"Successfully saved the data as '${configuration.format}' to '${configuration.path}' " +
          s"(Full configuration: ${configuration}).")
        data
      case Failure(ex) =>
        val message = s"Failed to save the data as '${configuration.format}' to '${configuration.path}' " +
          s"(Full configuration: ${configuration})."
        logError(message)
        throw new DataSinkException(message, ex)
    }
  }

  private def writing(writer: DataFrameWriter[Row], configuration: FileSinkConfiguration) = Try {
    configuration.buckets match {
      case Some(_) =>
        logInfo(s"Writing data to Hive as '${configuration.format}' in the '${configuration.path}' table. " +
          s"Notice that the path parameter is used as a table name in this case.")
        writer.saveAsTable(configuration.path)
      case None =>
        logInfo(s"Writing data as '${configuration.format}' to '${configuration.path}'.")
        writer.save(configuration.path)
    }
  }

  private def createWriter(data: DataFrame, configuration: FileSinkConfiguration): DataFrameWriter[Row] = {
    val writer = configuration.partitionFilesNumber match {
      case Some(partsNo) =>
        logDebug(s"Initializing the DataFrameWriter after repartitioning data to $partsNo partitions.")
        if (partsNo <= 2) data.repartition(partsNo).write
        else data.coalesce(partsNo).write
      case None => data.write
    }
    val partitionsWriter = configuration.partitionColumns match {
      case Nil => writer
      case partitions =>
        logDebug(s"Initializing the DataFrameWriter to partition the data using the following" +
          s" partition columns: [${partitions.mkString(", ")}].")
        writer.partitionBy(partitions: _*)
    }
    val bucketsWriter = configuration.buckets match {
      case None => partitionsWriter
      case Some(bc) =>
        logDebug(s"Initializing the DataFrameWriter to bucket the data into ${bc.number} buckets " +
          s"using the following partition columns: ${bc.bucketColumns.mkString(", ")}].")
        val sortedWriter = bc.sortByColumns match {
          case Nil => partitionsWriter
          case _ =>
            logDebug(s"Buckets will be sorted by the following columns: ${bc.sortByColumns.mkString(", ")}].")
            partitionsWriter.sortBy(bc.sortByColumns.head, bc.sortByColumns.tail: _*)
        }
        sortedWriter.bucketBy(bc.number, bc.bucketColumns.head, bc.bucketColumns.tail: _*)
    }

    bucketsWriter
      .mode(configuration.saveMode)
      .format(configuration.format.toString)
      .options(configuration.options)
  }
}
