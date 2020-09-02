package ro.esolutions.spark.io

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.spark.io.sinks.DataFrameSink.{FileDataFrameSink, JdbcDataFrameSink}
import ro.esolutions.spark.io.sinks.{FileDataSink, JdbcDataSink}
import ro.esolutions.spark.io.sinks.SinkConfiguration.{FileSinkConfiguration, JdbcSinkConfiguration}

class DataFrameSinkFactorySpec extends FlatSpec with Matchers with DataFrameSuiteBase {
  lazy val emptyDF: DataFrame = {
    import spark.implicits._
    Seq.empty[String].toDF()
  }

  "FileSinkConfiguration" should "create FileDataSource" in {
    val config = FileSinkConfiguration(
                    format = FormatType.Text,
                    path = "file:///tmp/")
    val result = dataSinkFactory(config, emptyDF)

    result shouldBe a[FileDataFrameSink]
    result.sink shouldBe a[FileDataSink]
    result.sink.configuration shouldBe(config)
    assertDataFrameEquals(emptyDF, result.data)
  }

  "JdbcSinkConfiguration" should "create JdbcDataSink" in {
    val table = "table"
    val url = "jdbc:postgresql://localhost/test"
    val config = JdbcSinkConfiguration(
      url = url,
      table = table,
      user = None,
      password = None,
      driver = None
    )
    val expectedSparkOption = Map(JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table)
    val result = dataSinkFactory(config, emptyDF)

    result shouldBe a[JdbcDataFrameSink]
    result.sink shouldBe a[JdbcDataSink]
    result.sink.configuration.writerOptions shouldBe expectedSparkOption
  }
}
