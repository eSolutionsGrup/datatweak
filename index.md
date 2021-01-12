![DataTweak](datatweak.png)
## Supported input
- json
- csv
- avro
- parquet
- hive

## Supported output
- avro
- parquet
- delta lake 
- hive

## Quickstart
This guide helps you quickly explore the main features of Data Tweak. 
It provides config snippets that show how to read, define the steps and queries of the ETL and write data.

### Config
DataTweak configurations is base on [PureConfig](https://pureconfig.github.io) which reads a config from: 
* a file in a file system
* resources in your classpath 
* an URL 
* a string

### Run
Usage: `spark-submit... <application-jar> [options]`
  
Available options:

| option       | description       |
|:-------------|:------------------|
|-j, --job <value>        | job is a required application name property |
|-n, --namespace <value>  | optional configuration namespace property |
|-u, --url <value>        | optional config url property |
|-l, --literal <value>    | optional literal config property |
