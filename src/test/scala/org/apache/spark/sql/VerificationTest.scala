package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite

class VerificationTest extends AnyFunSuite with ProcessProxy {
  val dummy = Seq(
    Dummy("a", "b", "c", 1, valid = true),
    Dummy("d", "e", "f", 2, valid = false),
    Dummy("g", "h", "i", 3, valid = true),
    Dummy("h", "g", "f", 4, valid = false),
    Dummy("e", "d", "c", 5, valid = true),
  )

  test("thing") {
    generates("n>2 | stats count() by valid",
      """(spark.table('x')
        |.where('(`n` > '2')')
        |.groupBy('valid')
        |.agg(F.expr('count() AS `count`')))
        |""".stripMargin)
  }

  test("thing2") {
    import spark.implicits._
    spark.createDataset(dummy).createOrReplaceTempView("x")
    executes("n>2",
      """+---+---+---+---+-----+
        ||  a|  b|  c|  n|valid|
        |+---+---+---+---+-----+
        ||  g|  h|  i|  3| true|
        ||  h|  g|  f|  4|false|
        ||  e|  d|  c|  5| true|
        |+---+---+---+---+-----+
        |""".stripMargin)
  }
}
