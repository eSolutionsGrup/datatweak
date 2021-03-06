package ro.esolutions.datatweak

import org.apache.spark.sql.SparkSession

trait JobRunnable[Context, Result] {
  def run(implicit spark: SparkSession, context: Context): Result
}
