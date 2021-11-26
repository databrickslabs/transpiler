package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._

case class Dummy(a: String, b: String, c: String, n: Int, valid: Boolean)

case class FakeData(id: Int,
                    gender: String,
                    email: String,
                    ipAddress: String,
                    array: Seq[String],
                    country: String,
                    cardType: String,
                    cardNumber: Long,
                    isStolen: Boolean,
                    timestamp: java.sql.Timestamp,
                    _raw: String)

case class FakeDataForJoin(id: Int, sport: String)


class SplExtensionTest extends AnyFunSuite with ProcessProxy {
  val dummy = Seq(
    Dummy("a", "b", "10.0.0.64", 1, valid = true),
    Dummy("d", "e", "10.0.0.65", 2, valid = false),
    Dummy("g", "h", "10.0.0.66", 3, valid = true),
    Dummy("h", "g", "10.0.0.67", 4, valid = false),
    Dummy("e", "d", "10.0.0.255", 5, valid = true)
  )

  test("it filters") {
    import spark.implicits._
    val result = spark.createDataset(dummy)
      .where("TERM('e') OR term('a')")
      .select("n")
    result.show()

    val rows = result.select("n").as[Int].collect()
    rows mustEqual Seq(1, 2, 5)
  }

  test("cidrmatch") {
    import spark.implicits._
    import org.apache.spark.sql.{functions => F}
    val result = spark.createDataset(dummy)
      .where(F.expr("cidr_match('10.0.0.128/4', c)"))
      .select("n")
    result.show()

    val rows = result.select("n").as[Int].collect()
    rows mustEqual Seq(5)
  }
}
