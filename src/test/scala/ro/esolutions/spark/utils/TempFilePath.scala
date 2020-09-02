package ro.esolutions.spark.utils

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.util.Try

trait TempFilePath extends BeforeAndAfterEach {
  this: Suite =>

  private var file: File = _
  private val tempDir = Option(System.getProperty("java.io.tmpdir")).getOrElse("/tmp")

  def tempPath: String = file.getAbsolutePath

  override def beforeEach(): Unit = {
    super.beforeEach()
    file = new File(s"$tempDir/spark_app_base_${UUID.randomUUID().toString}")
  }

  override def afterEach(): Unit = {
    Try(FileUtils.forceDelete(file))
    super.afterEach()
  }
}
