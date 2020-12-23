package ro.esolutions.datatweak.io.sinks

final case class DataSinkException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)
