package ro.esolutions.datatweak

import org.scalatest.{FlatSpec, Matchers}

class ArgsSpec extends FlatSpec with Matchers {

  it should "fails without arguments" in  {
    val args = Args(Array[String]())
    args shouldBe None
  }

  it should "loads the jobname" in {
    val args = Args(Array("-j", "JobName"))
    args.get.appName shouldBe "JobName"
  }

  it should "ignore UnknownArgument" in {
    val args = Args(Array("-j", "JobName", "unknownArgument"))
    args.get.appName shouldBe "JobName"
  }
}
