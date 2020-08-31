package ro.esolutions.spark

import java.net.URL

import scopt.OParser

import scala.util.Try

case class AppArgs(appName: String = "",
                   namespace: Option[String] = None,
                   url: Option[URL] = None,
                   literalConf: Option[String] = None)

object AppArgs {
  def apply(args: Array[String]): Option[AppArgs] = {
    val builder = OParser.builder[AppArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("spark-submit ... <application-jar> "),
        note("Available options:"),
        opt[String]('j', "job")
          .required()
          .action((v, cfg) => cfg.copy(appName = v))
          .text("job is a required application name property"),
        opt[String]('n', "namespace")
          .optional()
          .action((v, cfg) => cfg.copy(namespace = Some(v)))
          .text("optional configuration namespace property"),
        opt[String]('u', "url")
          .optional()
          .action((v, cfg) => cfg.copy(url = Try(new URL(v)).toOption))
          .text("optional config url property"),
        opt[String]('l', "literal")
          .optional()
          .action((v, cfg) => cfg.copy(literalConf = Some(v)))
          .text("optional literal config property"),
        help("help").text("prints this usage text")
      )
    }

    OParser.parse(parser, args, AppArgs())
  }
}
