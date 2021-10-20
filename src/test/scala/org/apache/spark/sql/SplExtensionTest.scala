package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._

case class Dummy(index: String, a: String, b: String, c: String, n: Int, valid: Boolean)
case class DummyKeyValue(index: String, key: String, value: String)
case class SingleRawField(index: String, _raw: String)

class SplExtensionTest extends AnyFunSuite {
  val dummy = Seq(
    Dummy("dummy", "a", "b", "c", 1, valid = true),
    Dummy("dummy", "d", "e", "f", 2, valid = false),
    Dummy("dummy", "g", "h", "i", 3, valid = true),
    Dummy("dummy", "h", "g", "f", 4, valid = false),
    Dummy("dummy", "e", "d", "c", 5, valid = true),
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
