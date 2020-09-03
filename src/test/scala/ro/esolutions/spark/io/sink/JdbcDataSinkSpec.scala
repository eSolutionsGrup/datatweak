package ro.esolutions.spark.io.sink

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.spark.io.sinks.SinkConfiguration.JdbcSinkConfiguration
import ro.esolutions.spark.utils.H2DatabaseCreator
import ro.esolutions.spark.implicits._
import ro.esolutions.spark.io._
import ro.esolutions.spark.io.sinks.DataSinkException

class JdbcDataSinkSpec extends FlatSpec with Matchers with H2DatabaseCreator with DataFrameSuiteBase {

  lazy val inputData = {
    spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/test/resources/data/csv/users.csv")
  }

  val config = JdbcSinkConfiguration(
    url = h2url,
    table = table,
    user = Some(user),
    password = Some(password),
    driver = Some(driver)
  )

  it should "saving with SaveMode.default if table not exists" in {
    noException shouldBe thrownBy(inputData.sink(config).write)

    val result: DataFrame = spark.read.format("jdbc").options(config.writerOptions).load()
    assertDataFrameEquals(inputData, result)
  }

  it should "fail with SaveMode.default if table already exists" in {
    createDatabases(jdbcConnection)
    a[DataSinkException] shouldBe thrownBy(inputData.sink(config).write)
  }

  it should "saving if table already exists and SaveMode is not 'default'" in {
    createDatabases(jdbcConnection)
    noException shouldBe thrownBy(inputData.sink(config.copy(saveMode = "overwrite")).write)

    val result: DataFrame = spark.read.format("jdbc").options(config.writerOptions).load()
    assertDataFrameEquals(inputData, result)
  }
}
