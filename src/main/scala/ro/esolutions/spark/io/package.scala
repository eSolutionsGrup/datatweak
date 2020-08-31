package ro.esolutions.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import pureconfig.ConfigReader
import ro.esolutions.spark.io.sinks.DataFrameSink._
import ro.esolutions.spark.io.sinks._
import ro.esolutions.spark.io.sinks.SinkConfiguration._
import ro.esolutions.spark.io.sources.SourceConfiguration._
import ro.esolutions.spark.io.sources._

package object io {
  trait FormatAware { def format: FormatType }
  trait DataSourceConfiguration
  trait DataSinkConfiguration

  trait FormatAwareDataSourceConfiguration extends DataSourceConfiguration with FormatAware
    with Product with Serializable
  trait FormatAwareDataSinkConfiguration extends DataSinkConfiguration with FormatAware
    with Product with Serializable

  trait DataSource[Config <: DataSourceConfiguration] {
    def configuration: Config
    def read(implicit spark: SparkSession): DataFrame
  }
  trait DataSourceFactory {
    def apply[Config <: DataSourceConfiguration](configuration: Config): DataSource[Config]
  }

  trait DataSink[Config <: DataSinkConfiguration, WriteOut] {
    def configuration: Config
    def write(data: DataFrame): WriteOut
  }

  trait DataFrameSinkFactory {
    def apply[Config <: DataSinkConfiguration, R](config: Config, data: DataFrame): DataFrameSink[Config, R]
  }

  implicit val structTypeReader = ConfigReader[String].map(json => DataType.fromJson(json).asInstanceOf[StructType])
  implicit val formatTypeReader = ConfigReader[String].map(format => FormatType.fromString(format).get)

  implicit val dataSourceFactory = new DataSourceFactory {
    override def apply[Config <: DataSourceConfiguration](configuration: Config): DataSource[Config] = {
      configuration match {
        case c: FileSourceConfiguration => FileDataSource(c).asInstanceOf[DataSource[Config]]
        case c: JdbcSourceConfiguration => JdbcDataSource(c).asInstanceOf[DataSource[Config]]
        case c => throw new IllegalArgumentException(s"Unsupported configuration type ${c.getClass}")
      }
    }
  }

  implicit val dataSinkFactory = new DataFrameSinkFactory {
    override def apply[Config <: DataSinkConfiguration, WriteOut](configurator: Config, data: DataFrame) = {
      configurator match {
        case c: FileSinkConfiguration => FileDataFrameSink(c, data).asInstanceOf[DataFrameSink[Config, WriteOut]]
        case c: JdbcSinkConfiguration => JdbcDataFrameSink(c, data).asInstanceOf[DataFrameSink[Config, WriteOut]]
        case c => throw new IllegalArgumentException(s"Unsupported configuration type ${c.getClass}")
      }
    }
  }
}
