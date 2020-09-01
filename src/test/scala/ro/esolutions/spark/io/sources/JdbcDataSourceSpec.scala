package ro.esolutions.spark.io.sources

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.spark.io.sources.SourceConfiguration.JdbcSourceConfiguration
import ro.esolutions.spark.implicits._
import ro.esolutions.spark.utils.H2DatabaseCreator

class JdbcDataSourceSpec extends FlatSpec with Matchers with DataFrameSuiteBase with H2DatabaseCreator {

  val schema = StructType(Seq(
    StructField("ID", DataTypes.IntegerType),
    StructField("NAME", DataTypes.StringType),
    StructField("AGE", DataTypes.IntegerType)
  ))

  val config = JdbcSourceConfiguration(url = h2url,
    table = table,
    user = Some(user),
    password = Some(password),
    driver = Some(driver))

  it should "reading" in {
    val persons = Seq((1, "neghina", 40), (2, "john", 35))
    insertTable(jdbcConnection, persons)

    val result = JdbcDataSource(config).read(spark)
    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(persons.map(p => Row(p._1, p._2, p._3))),
      schema
    )

    assertDataFrameEquals(expected, result)
    noException shouldBe thrownBy(spark.source(config).read(spark))
  }

  it should "fail if table not found" in {
    a[DataSourceException] shouldBe thrownBy(JdbcDataSource(config.copy(table = "unknown")).read(spark))
  }
}
