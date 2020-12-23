package ro.esolutions.datatweak.io.sources

final case class DataSourceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)
