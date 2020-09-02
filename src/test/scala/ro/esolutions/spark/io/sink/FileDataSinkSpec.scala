package ro.esolutions.spark.io.sink

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.spark.implicits._
import ro.esolutions.spark.io.{FormatType, _}
import ro.esolutions.spark.io.sinks.DataSinkException
import ro.esolutions.spark.io.sinks.SinkConfiguration.FileSinkConfiguration
import ro.esolutions.spark.utils.TempFilePath

class FileDataSinkSpec extends FlatSpec with Matchers with DataFrameSuiteBase with TempFilePath {

  lazy val inputData = {
    import spark.implicits._
    Seq(("1", "Lucian", "Neghina", "M"),
      ("2", "Eliza", "Popescu", "F"),
      ("3", "Wesley", "Taylor", "M"),
      ("4", "Peyton", "Fuller", "F")).toDF("id", "first_name", "last_name", "sex")
  }
  val format = FormatType.Parquet

  it should "saving the input data" in {
    val sinkConfig = FileSinkConfiguration(format, tempPath)
    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val writtenData: DataFrame = spark.read.parquet(tempPath)
    assertDataFrameEquals(inputData.orderBy("id"), writtenData.orderBy("id"))
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
    assertDataFrameEquals(inputData.orderBy("id"), writtenData.orderBy("id"))
  }

  it should "saving the input partitioned" in {
    val partition = "sex"
    val sinkConfig = FileSinkConfiguration(
      format = format,
      path = tempPath,
      partitionColumns = Seq(partition))

    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val writtenData: DataFrame = spark.read.parquet(tempPath)
    assertDataFrameEquals(inputData.orderBy("id"), writtenData.orderBy("id"))

    val filePartitions = tempFile.listFiles().filter(_.getPath.contains(s"/$partition="))
    filePartitions.size should be > 0
  }

  it should "saving maximum number of partitions" in {
    val partition = 3
    val sinkConfig = FileSinkConfiguration(
      format = format,
      path = tempPath,
      partitionFilesNumber = Some(partition))

    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val writtenData: DataFrame = spark.read.parquet(tempPath)
    assertDataFrameEquals(inputData.orderBy("id"), writtenData.orderBy("id"))

    val filePartitions = tempFile.listFiles().filter(_.getPath.endsWith("parquet"))
    filePartitions.size shouldBe(partition)
  }

  it should "saving in Hive" in {
    val tableName = "test_tbl"
    val sinkConfig = FileSinkConfiguration(
      format = format,
      path = tableName,
      buckets = Some(Buckets(1, Seq("sex"))))

    noException shouldBe thrownBy(inputData.sink(sinkConfig).write)

    val writtenData: DataFrame = spark.sql(s"select * from $tableName")
    assertDataFrameEquals(inputData.orderBy("id"), writtenData.orderBy("id"))
  }

}
