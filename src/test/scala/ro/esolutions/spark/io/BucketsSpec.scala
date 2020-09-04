package ro.esolutions.spark.io

import org.scalatest.{FlatSpec, Matchers}

class BucketsSpec extends FlatSpec with Matchers {

  it should "be mocked" in {
    val bucket = Buckets(5, Seq("columnName"), Seq("sortColumn"))
    bucket.number shouldBe(5)
    bucket.bucketColumns.size should be > 0
  }

  it should "fail" in {
    a[IllegalArgumentException] shouldBe thrownBy(Buckets(-5, Seq("columnName"), Seq("sortColumn")))
    a[IllegalArgumentException] shouldBe thrownBy(Buckets(5, Seq(), Seq("sortColumn")))
    noException shouldBe thrownBy(Buckets(5, Seq("columnName")))
  }

}
