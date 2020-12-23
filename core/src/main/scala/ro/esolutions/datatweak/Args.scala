package ro.esolutions.datatweak

import java.net.URL

import scopt.{DefaultOParserSetup, OParser}

import scala.util.Try

case class Args(appName: String = "",
                namespace: Option[String] = None,
                url: Option[URL] = None,
                literalConf: Option[String] = None)

object Args {
  def apply(args: Array[String]): Option[Args] = {
    val builder = OParser.builder[Args]
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

    val setup = new DefaultOParserSetup() {
      override def showUsageOnError: Option[Boolean] = Some(true)
      override def errorOnUnknownArgument: Boolean = false
    }

    OParser.parse(parser, args, Args(), setup)
  }
}
