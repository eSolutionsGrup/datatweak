package ro.esolutions.spark.utils

import java.io.FileReader
import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

import org.apache.spark.internal.Logging
import org.h2.tools.RunScript
import org.scalatest.{BeforeAndAfterAll, Suite}

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

  createDatabases(jdbcConnection)

  override def afterAll(): Unit = {
    if (jdbcConnection != null) {
      dropTables(jdbcConnection)
      jdbcConnection.close()
    }
  }

  def insertTable(jdbcConnection: Connection, data: Seq[(Int, String, Int)]): Unit = {
    val query = "INSERT INTO persons (id, name, age) VALUES ( ?, ?, ?);"
    val ps: PreparedStatement = jdbcConnection.prepareStatement(query)
    data.foreach { p =>
      ps.setInt(1, p._1)
      ps.setString(2, p._2)
      ps.setInt(3, p._3)
      ps.executeUpdate()
    }
    jdbcConnection.commit()
  }

  private def createDatabases(jdbcConnection: Connection): Unit = {
    log.info("Creating tables in test databases")
    RunScript.execute(jdbcConnection, new FileReader(getClass.getResource("/data/createTables.sql").getPath))
  }

  private def dropTables(jdbcConnection: Connection): Unit = {
    log.info("Dropping all tables from test databases")
    RunScript.execute(jdbcConnection, new FileReader(getClass.getResource("/data/dropTables.sql").getPath))
  }
}
