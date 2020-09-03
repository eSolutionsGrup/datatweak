package ro.esolutions.spark.utils

import java.io.FileReader
import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.h2.tools.RunScript
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Try

trait H2DatabaseCreator extends BeforeAndAfterAll with Logging {
  this: Suite =>

  val h2url = "jdbc:h2:mem:test;"
  val driver = "org.h2.Driver"
  val table = "persons"
  val user = ""
  val password = ""

  Class.forName("org.h2.Driver")
  val jdbcConnection: Connection = DriverManager.getConnection(h2url)
  val stmt: Statement = jdbcConnection.createStatement()

  val schema = StructType(Seq(
    StructField("ID", DataTypes.IntegerType),
    StructField("FIRST_NAME", DataTypes.StringType),
    StructField("LAST_NAME", DataTypes.StringType),
    StructField("GENDER", DataTypes.StringType)
  ))

  override def afterAll(): Unit = {
    if (jdbcConnection != null) {
      Try(dropTables(jdbcConnection))
      jdbcConnection.close()
    }
  }

  def insertTable(jdbcConnection: Connection, data: Seq[(Int, String, String, String)]): Unit = {
    val query = "INSERT INTO persons (id, first_name, last_name, gender) VALUES ( ?, ?, ?, ?);"
    val ps: PreparedStatement = jdbcConnection.prepareStatement(query)
    data.foreach { p =>
      ps.setInt(1, p._1)
      ps.setString(2, p._2)
      ps.setString(3, p._3)
      ps.setString(4, p._4)
      ps.executeUpdate()
    }
    jdbcConnection.commit()
  }

  def createDatabases(jdbcConnection: Connection): Unit = {
    log.info("Creating tables in test databases")
    RunScript.execute(jdbcConnection, new FileReader(getClass.getResource("/data/createTables.sql").getPath))
  }

  def dropTables(jdbcConnection: Connection): Unit = {
    log.info("Dropping all tables from test databases")
    RunScript.execute(jdbcConnection, new FileReader(getClass.getResource("/data/dropTables.sql").getPath))
  }
}
