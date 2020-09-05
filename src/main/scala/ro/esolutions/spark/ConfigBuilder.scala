package ro.esolutions.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import pureconfig.{ConfigObjectSource, ConfigSource}

trait ConfigBuilder extends Logging {

  private[spark] def createConfiguration(args: Args): ConfigSource = {

    log.info(s"${args.appName} build config:\n$args")

    val literalConfiguration: Option[ConfigObjectSource] = args.literalConf.map(l => ConfigSource.string(l))
    val urlConfiguration: Option[ConfigObjectSource] = args.url.map(u => ConfigSource.url(u))

    val configurationSources = Seq(literalConfiguration, urlConfiguration, Some(ConfigSource.defaultApplication))

    val confObj = configurationSources.collect{ case Some(config) => config }
      .fold(ConfigSource.empty)((x, y) => x.withFallback(y))

    args.namespace
      .map(n => confObj.at(n))
      .getOrElse(confObj)
  }

}
