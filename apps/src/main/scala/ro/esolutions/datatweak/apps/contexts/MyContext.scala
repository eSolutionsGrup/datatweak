package ro.esolutions.datatweak.apps.contexts

import ro.esolutions.datatweak.io.sinks.SinkConfiguration
import ro.esolutions.datatweak.io.sources.SourceConfiguration

case class MyContext(input: SourceConfiguration, output: SinkConfiguration)