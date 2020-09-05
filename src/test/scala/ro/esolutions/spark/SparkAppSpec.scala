package ro.esolutions.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._

class SparkAppSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  val args = Seq("-j", "testJobName").toArray

  "SparkApp.main" should "successfully completes" in {
    noException shouldBe thrownBy(MockApp.main(args))
    spark.sparkContext.isStopped shouldBe true
  }

  "SparkApp.main" should "fails without job name argument" in {
    a[IllegalArgumentException] shouldBe thrownBy(MockApp.main(Array()))
  }

  "SparkApp.main" should "fails if SparkApp.createContext fails" in {
    a[MockApException] shouldBe thrownBy(MockAppFailureConfig.main(args))
    spark.sparkContext.isStopped shouldBe true
  }

  "SparkApp.main" should "fails if SparkApp.run fails" in {
    a[MockApException] shouldBe thrownBy(MockFailureAppConfig.main(args))
    spark.sparkContext.isStopped shouldBe true
  }

  object MockApp extends SparkApp[String, Unit] {
    override def createContext(conf: ConfigSource): String = "Test"
    override def run(implicit spark: SparkSession, context: String): Unit = Unit
  }

  object MockAppFailureConfig extends SparkApp[String, Unit] {
    override def createContext(conf: ConfigSource): String = throw new MockApException
    override def run(implicit spark: SparkSession, context: String): Unit = Unit
  }

  object MockFailureAppConfig extends SparkApp[String, Unit] {
    def createContext(config: ConfigSource): String = "Test"
    override def run(implicit spark: SparkSession, config: String): Unit = throw new MockApException
  }

  class MockApException extends Exception
}
