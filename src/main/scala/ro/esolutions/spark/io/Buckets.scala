package ro.esolutions.spark.io

case class Buckets(number: Int, bucketColumns: Seq[String], sortByColumns: Seq[String] = Seq())
