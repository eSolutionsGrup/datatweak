package ro.esolutions.spark

import org.scalatest.{FlatSpec, Matchers}

class AppArgsSpec extends FlatSpec with Matchers {

  it should "fails without arguments" in  {
      a[NoSuchElementException] shouldBe thrownBy(AppArgs(Array[String]()))
  }

  it should "loads the jobname" in {
    val args = AppArgs(Array("-j", "JobName"))
    args.appName shouldBe "JobName"
  }

  it should "ignore UnknownArgument" in {
    val args = AppArgs(Array("-j", "JobName", "unknownArgument"))
    args.appName shouldBe "JobName"
  }
}
