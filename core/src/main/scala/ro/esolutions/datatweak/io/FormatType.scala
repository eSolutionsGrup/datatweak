package ro.esolutions.datatweak.io

import scala.util.{Success, Try}

sealed trait FormatType

object FormatType {

  private val JdbcFormat = "jdbc"

  def fromString(formatString: String): Try[FormatType] = formatString.trim match {
    case JdbcFormat => Success(Jdbc)
    case format => Success(FileFormat(format))
  }

  final case object Jdbc extends FormatType { override def toString: String = JdbcFormat }
  final case class FileFormat(format: String) extends FormatType { override def toString: String = format }

}
