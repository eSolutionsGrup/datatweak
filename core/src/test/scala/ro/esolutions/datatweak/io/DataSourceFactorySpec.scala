package ro.esolutions.datatweak.io

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.datatweak.io.sources.SourceConfiguration.{FileSourceConfiguration, JdbcSourceConfiguration}
import ro.esolutions.datatweak.io.sources.{FileDataSource, JdbcDataSource}

class DataSourceFactorySpec extends FlatSpec with Matchers {
  "FileSourceConfiguration" should "create FileDataSource" in {
    val config = FileSourceConfiguration(FormatType.FileFormat("text"), "file:///tmp/", None, Map())
    val result = dataSourceFactory(config)

    result shouldBe a[FileDataSource]
    result.configuration shouldBe(config)
  }

  "JdbcSourceConfiguration" should "create JdbcDataSource" in {
    val table = "table"
    val url = "jdbc:postgresql://localhost/test"
    val config = JdbcSourceConfiguration(
      url = url,
      table = table,
      user = None,
      password = None,
      driver = None
    )
    val expectedSparkOption = Map(JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table)
    val result = dataSourceFactory(config)

    result shouldBe a[JdbcDataSource]
    result.configuration shouldBe config
    result.configuration.readerOptions shouldBe expectedSparkOption
  }
}
