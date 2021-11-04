package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._

case class Dummy(a: String, b: String, c: String, n: Int, valid: Boolean)
case class DummyWithArray(a: String, b: String, c: String, d: Seq[String], n: Int, valid: Boolean)
case class DummyWithIntArray(a: String, b: String, c: String, d: Seq[Int], n: Int, valid: Boolean)
case class DummyKeyValue(key: String, value: String)
case class SingleRawField(_raw: String)
case class CountryByContinent(continent: String, country: String)
case class Flow(ts: java.sql.Timestamp, src_ip: String, dst_ip: String, service: String)

class SplExtensionTest extends AnyFunSuite {
  val dummy = Seq(
    Dummy("a", "b", "c", 1, valid = true),
    Dummy("d", "e", "f", 2, valid = false),
    Dummy("g", "h", "i", 3, valid = true),
    Dummy("h", "g", "f", 4, valid = false),
    Dummy("e", "d", "c", 5, valid = true),
  )

  test("it filters") {
    val spark = SparkSession.builder()
      .withExtensions(e => new SplExtension().apply(e))
      .master("local[1]")
      .getOrCreate()
    import spark.implicits._

    val result = spark.createDataset(dummy)
      .where("TERM('e') OR term('a')")
      .select("n")
    result.show()

    val rows = result.select("n").as[Int].collect()
    rows mustEqual Seq(1, 2, 5)
  }
}

object x
