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

* * *

DataTweak configurations is base on [PureConfig](https://pureconfig.github.io) which reads a config from: 
* a file in a file system
* resources in your classpath 
* an URL 
* a string

### Data ingest

Read a CSV with header using schema and save to avro format.
```
    input: {
        format = "csv"
        path = "file:///datasets/users.csv"
        options = {
        "header": "true"
        }
        schema = """{
            "type": "struct",
            "fields": [{
              "name": "id",
              "type": "integer",
              "nullable": false
            }, {
              "name": "name",
              "type": "string",
              "nullable": false
            }, {
              "name": "age",
              "type": "integer",
              "nullable": true
            }]
          }"""
    }
    output: {
        format = "avro"
        path = "file://bootcamp/avro/"
    }
```


### Data wrangling 

Read tow avro files, join it and save to parquet format.
```
    source: [
        {
            "name": "orders"
            input: {
                format = "avro"
                path = "file:///home/lucian/workspace/bigdata/datasets/retail/warehouse/orders/"
            }
        },
        {
            "name": "order_items"
            input: {
                format = "avro"
                path = "file:///home/lucian/workspace/bigdata/datasets/retail/warehouse/order_items/"
            }
        }
    ]
    query: "SELECT * FROM orders o JOIN order_items i ON (o.order_id == i.order_item_order_id)"
    output: {
        format = "parquet"
        path = "file:///tmp/bootcamp/parquet/"
    }
```

### Run

* * *

Usage: `spark-submit... <application-jar> [options]`
  
Available options:

| option       | description       |
|:-------------|:------------------|
|-j, --job <value>        | job is a required application name property |
|-n, --namespace <value>  | optional configuration namespace property |
|-u, --url <value>        | optional config url property |
|-l, --literal <value>    | optional literal config property |
