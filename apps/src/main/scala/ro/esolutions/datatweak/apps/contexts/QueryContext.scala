package ro.esolutions.datatweak.apps.contexts

import ro.esolutions.datatweak.io.sinks.SinkConfiguration
import ro.esolutions.datatweak.io.sources.SourceConfiguration

case class DataSource(name: String, input: SourceConfiguration)

case class QueryContext(source: List[DataSource], output: SinkConfiguration, query: String)