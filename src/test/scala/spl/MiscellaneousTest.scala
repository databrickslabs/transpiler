package spl

import spl.ast.CommandOptions
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

class MiscellaneousTest extends AnyFunSuite with PrivateMethodTester {

  var testCmdOps: CommandOptions = CommandOptions(Seq(
    ast.FC("fieldA", ast.IntValue(1)),
    ast.FC("fieldB", ast.IntValue(2)),
    ast.FC("fieldC", ast.TimeSpan(1, "2022-08-03T14:00:00+00:00"))
  ))

  test("CommandOptions toMap() method should returns a map") {

    val testOptionsMap = testCmdOps.toMap
    assert(testOptionsMap("fieldA") == ast.IntValue(1))
    assert(testOptionsMap("fieldB") == ast.IntValue(2))
  }

  test("CommandOptions throwIAE() method should raise an IllegalArgumentException") {
    val throwIAE = PrivateMethod[CommandOptions]('throwIAE)
    assertThrows[IllegalArgumentException] {
      testCmdOps invokePrivate throwIAE("This is an error message")
    }
  }

  test("CommandOptions getSpan() method should raise an IllegalArgumentException") {
    val caught = intercept[IllegalArgumentException] {
      testCmdOps.getSpanOption("fieldA")
    }
    assert(caught.getMessage == "not a span: IntValue(1)")
  }

  test("CommandOptions getSpan() method should return a SplSpan") {
    assert(testCmdOps.getSpanOption("fieldC").get == ast.TimeSpan(1, "2022-08-03T14:00:00+00:00"))
  }

  test("Equals operator toString() method should return `=`") {
    val testEquals = ast.Equals
    assert(testEquals.toString == "=" )
  }

  test("And operator toString() method should return `AND`") {
    val testAnd = ast.And
    assert(testAnd.toString == "AND" )
  }

  test("InList operator toString() method should return `IN`") {
    val testIn = ast.InList
    assert(testIn.toString == "IN" )
  }

  test("UnaryNot operator precedence() method should return `2`") {
    val testUnaryNot = ast.UnaryNot
    assert(testUnaryNot.precedence == 2)
  }

  test("CommandOptions getIntOption() method raise an `IllegalArgumentException`") {
    assertThrows[IllegalArgumentException] {
      testCmdOps.getIntOption("fieldC")
    }
  }

  test("CommandOptions getStringOption() method raise an `IllegalArgumentException`") {
    assertThrows[IllegalArgumentException] {
      testCmdOps.getStringOption("fieldC")
    }
  }
}
