package ro.esolutions.spark.io.sink

import org.apache.spark.sql.SaveMode
import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import ro.esolutions.spark.io.{Buckets, FormatType}
import ro.esolutions.spark.io.sinks.SinkConfiguration
import ro.esolutions.spark.io.sinks.SinkConfiguration.{FileSinkConfiguration, JdbcSinkConfiguration}
import ro.esolutions.spark.io.sources.SourceConfiguration

class SinkConfigurationSpec extends FlatSpec with Matchers {

  it should "auto loading file config" in {
    val source = ConfigSource.string("""{ "format" = "parquet", "path" = "file:///tmp" }""")
    val result = source.loadOrThrow[SinkConfiguration]

    result shouldBe a[FileSinkConfiguration]
    result.format shouldBe FormatType.Parquet
    result.asInstanceOf[FileSinkConfiguration].saveMode shouldBe "default"
  }

  it should "auto loading hive config" in {
    val source = ConfigSource.string(
      """{  "format" = "parquet",
        |   "path" = "file:///tmp",
        |   buckets = {
        |            number = 3
        |            bucket-columns = ["id"]
        |        }}""".stripMargin)
    val result = source.loadOrThrow[SinkConfiguration]

    result shouldBe a[FileSinkConfiguration]
    result.format shouldBe FormatType.Parquet
    result.asInstanceOf[FileSinkConfiguration].saveMode shouldBe "default"
    result.asInstanceOf[FileSinkConfiguration].buckets shouldBe Some(Buckets(3, Seq("id")))
  }

  it should "auto loading jdbc config" in {
    val source = ConfigSource.string(
      """{  url = "jdbc://jdbcUrl"
        |   table = "tbl"}""".stripMargin)
    val result = source.loadOrThrow[SinkConfiguration]

    result shouldBe a[JdbcSinkConfiguration]
    result.format shouldBe FormatType.Jdbc
  }

  it should "fails without all required fields" in {
    val source = ConfigSource.string("""{"format" = "csv"}""".stripMargin)
    a[RuntimeException] shouldBe thrownBy(source.loadOrThrow[SinkConfiguration])
  }
}
