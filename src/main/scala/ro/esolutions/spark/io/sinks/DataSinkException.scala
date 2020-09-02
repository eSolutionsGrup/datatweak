package ro.esolutions.spark.io.sinks

final case class DataSinkException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)
