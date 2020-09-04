package ro.esolutions.spark.io.sources

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.spark.io.FormatType
import ro.esolutions.spark.io.sources.SourceConfiguration.FileSourceConfiguration
import ro.esolutions.spark.implicits._

class FileDataSourceSpec extends FlatSpec with Matchers with DataFrameSuiteBase {

  import spark.implicits._

  it should "reading with inferring the schema" in {
    val inputPath = "src/test/resources/data/json/persons.json"
    val options = Map(
      "columnNameOfCorruptRecord" -> "_corrupt_record",
      "mode" -> "FAILFAST",
      "multiLine" -> "true")
    val config = FileSourceConfiguration(FormatType.FileFormat("json"), inputPath, None, options)

    val result1 = FileDataSource(config).read(spark)
    val expected = Seq(("Lucian", "Neghina", "M")).toDF("first_name", "last_name", "sex")

    assertDataFrameEquals(expected, result1)
    noException shouldBe thrownBy(spark.source(config).read(spark))
  }

  it should "reading with schema" in {
    val inputPath = "src/test/resources/data/json/persons.json"
    val options = Map(
      "columnNameOfCorruptRecord" -> "_corrupt_record",
      "mode" -> "FAILFAST",
      "multiLine" -> "true")
    val schema = StructType(Seq(
      StructField("first_name", DataTypes.StringType),
      StructField("last_name", DataTypes.StringType),
      StructField("sex", DataTypes.StringType)
    ))
    val config = FileSourceConfiguration(FormatType.FileFormat("json"), inputPath, Some(schema), options)

    val result = FileDataSource(config).read(spark)
    val expected = Seq(("Lucian", "Neghina", "M")).toDF("first_name", "last_name", "sex")

    assertDataFrameEquals(expected, result)
    noException shouldBe thrownBy(spark.source(config).read(spark))
  }

  it should "fails if the file does not exist" in {
    val inputPath = "unknown/path/to/inexistent/file.txt"
    val config = FileSourceConfiguration(FormatType.FileFormat("text"), inputPath, None)
    a[DataSourceException] shouldBe thrownBy(FileDataSource(config).read(spark))
    a[DataSourceException] shouldBe thrownBy(spark.source(config).read(spark))

    assert(FileDataSource(config).configuration, config)
  }
}
