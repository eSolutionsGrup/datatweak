package ro.esolutions.spark.io.sources

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import pureconfig.generic.FirstSuccessCoproductHint
import ro.esolutions.spark.io._

sealed trait SourceConfiguration extends FormatAwareDataSourceConfiguration {
  def options: Map[String, String]
  def schema: Option[StructType]
}

object SourceConfiguration {
  implicit val sourceConfigurationHint = new FirstSuccessCoproductHint[SourceConfiguration]

  final case class FileSourceConfiguration(format: FormatType,
                                           path: String,
                                           schema: Option[StructType],
                                           options: Map[String, String] = Map()) extends SourceConfiguration
  final case class JdbcSourceConfiguration(url: String,
                                           table: String,
                                           user: Option[String],
                                           password: Option[String],
                                           driver: Option[String],
                                           options: Map[String, String] = Map(),
                                           schema: Option[StructType] = None) extends SourceConfiguration {
    val format = FormatType.Jdbc
    def readerOptions: Map[String, String] = {
      val userOption = user.map(v => Map("user" -> v)).getOrElse(Nil)
      val passwordOption = password.map(v => Map("password" -> v)).getOrElse(Nil)
      val driverOption = driver.map(v => Map("driver" -> v)).getOrElse(Nil)
      options + (JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table) ++
        userOption ++ passwordOption ++ driverOption
    }
  }
}
