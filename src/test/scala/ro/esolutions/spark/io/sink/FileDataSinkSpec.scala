package ro.esolutions.spark.io.sink

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.spark.io.FormatType
import ro.esolutions.spark.io.sinks.SinkConfiguration.FileSinkConfiguration
import ro.esolutions.spark.utils.TempFilePath
import ro.esolutions.spark.implicits._
import ro.esolutions.spark.io._
import ro.esolutions.spark.io.sinks.DataSinkException

class FileDataSinkSpec extends FlatSpec with Matchers with DataFrameSuiteBase with TempFilePath {

  lazy val inputData = {
    import spark.implicits._
    Seq(("Lucian", "Neghina", "M")).toDF("first_name", "last_name", "sex")
  }
  val format = FormatType.Parquet

  it should "saving the input data" in {
    val sinkConfig = FileSinkConfiguration(format, tempPath)
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val writtenData: DataFrame = spark.read.parquet(tempPath)
    assertDataFrameEquals(inputData, writtenData)
  }

  it should "fail if file already exists and the SaveMode.default" in {
    val sinkConfig = FileSinkConfiguration(format, tempPath)
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)
    a[DataSinkException] should be thrownBy (inputData.sink(sinkConfig).write)
  }

  it should "saving if file already exists and the SaveMode.Overwrite" in {
    val sinkConfig = FileSinkConfiguration(format = format, path = tempPath, saveMode = "overwrite")
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val writtenData: DataFrame = spark.read.parquet(tempPath)
    assertDataFrameEquals(inputData, writtenData)
  }


}
