package ro.esolutions.spark.io

case class Buckets(number: Int, bucketColumns: Seq[String], sortByColumns: Seq[String] = Seq()) {
  require(number > 0, "it should be positive")
  require(bucketColumns.size > 0, "it should have minimum one column")
}
