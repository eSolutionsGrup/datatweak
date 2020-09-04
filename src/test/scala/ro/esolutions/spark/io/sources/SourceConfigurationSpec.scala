package ro.esolutions.spark.io.sources

import org.scalatest.{FlatSpec, Matchers}
import pureconfig.error.ConfigReaderException
import pureconfig._
import pureconfig.generic.auto._
import ro.esolutions.spark.implicits._
import ro.esolutions.spark.io._
import ro.esolutions.spark.io.sources.SourceConfiguration.{FileSourceConfiguration, JdbcSourceConfiguration}

class SourceConfigurationSpec extends FlatSpec with Matchers {

  it should "auto loading file config" in {
    val source = ConfigSource.string("""{ "format" = "csv", "path" = "file:///tmp" }""")
    val result = source.loadOrThrow[SourceConfiguration]

    result shouldBe a[FileSourceConfiguration]
    result.format shouldBe FormatType.FileFormat("csv")
  }

  it should "auto loading jdbc config" in {
    val source = ConfigSource.string("""{url = "jdbc://sdsdsa", table = "tbl" }""".stripMargin)
    val result = source.loadOrThrow[SourceConfiguration]

    result shouldBe a[JdbcSourceConfiguration]
    result.format shouldBe FormatType.Jdbc
  }

  "jdbc config" should "always has the format FormatType.Jdbc" in {
    val source = ConfigSource.string(
      """{"format" = "csv"
        |  url = "jdbc://jdbc_url"
        |  table = "tbl" }""".stripMargin)
    val result = source.loadOrThrow[SourceConfiguration]

    result shouldBe a[JdbcSourceConfiguration]
    result.format shouldBe FormatType.Jdbc
  }

  it should "fails without all required fields" in {
    val source = ConfigSource.string("""{"format" = "csv"}""".stripMargin)
    a[RuntimeException] shouldBe thrownBy(source.loadOrThrow[SourceConfiguration])
  }
}
