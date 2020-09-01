package ro.esolutions.spark.io.sinks

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import pureconfig.generic.FirstSuccessCoproductHint
import ro.esolutions.spark.io.{Buckets, FormatAwareDataSinkConfiguration, FormatType}

sealed trait SinkConfiguration extends FormatAwareDataSinkConfiguration

object SinkConfiguration {
  implicit val sinkConfigurationHint = new FirstSuccessCoproductHint[SinkConfiguration]

  final case class FileSinkConfiguration(format: FormatType,
                                        path: String,
                                        saveMode: String = "default",
                                        partitionFilesNumber: Option[Int] = None,
                                        partitionColumns: Seq[String] = Seq(),
                                        buckets: Option[Buckets] = None,
                                        options: Map[String, String] = Map()) extends SinkConfiguration

  case class JdbcSinkConfiguration(url: String,
                                   table: String,
                                   user: Option[String],
                                   password: Option[String],
                                   driver: Option[String],
                                   saveMode: String = "default",
                                   options: Map[String, String] = Map()) extends SinkConfiguration {
    val format = FormatType.Jdbc
    def writerOptions: Map[String, String] = {
      val userOption = user.map(v => Map("user" -> v)).getOrElse(Nil)
      val passwordOption = password.map(v => Map("password" -> v)).getOrElse(Nil)
      val driverOption = driver.map(v => Map("driver" -> v)).getOrElse(Nil)
      options + (JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table) ++
        userOption ++ passwordOption ++ driverOption
    }
  }
}
