package ro.esolutions.spark.io.sources

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ro.esolutions.spark.io.sources.SourceConfiguration.JdbcSourceConfiguration
import ro.esolutions.spark.implicits._
import ro.esolutions.spark.utils.H2DatabaseCreator

class JdbcDataSourceSpec extends FlatSpec with Matchers with DataFrameSuiteBase with H2DatabaseCreator with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    createDatabases(jdbcConnection)
    super.beforeAll()
  }

  val config = JdbcSourceConfiguration(url = h2url,
    table = table,
    user = Some(user),
    password = Some(password),
    driver = Some(driver))

  it should "reading" in {
    val persons = Seq(
      (1, "enrique", "rodriquez", "male"),
      (2, "krin", "newman", "female")
    )
    insertTable(jdbcConnection, persons)

    val result = JdbcDataSource(config).read(spark)
    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(persons.map(p => Row(p._1, p._2, p._3, p._4))),
      schema
    )

    assertDataFrameEquals(expected, result)
    noException shouldBe thrownBy(spark.source(config).read(spark))
  }

  it should "fail if table not found" in {
    a[DataSourceException] shouldBe thrownBy(JdbcDataSource(config.copy(table = "unknown")).read(spark))
  }
}
