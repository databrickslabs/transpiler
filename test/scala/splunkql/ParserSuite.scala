package splunkql

import fastparse.internal.Instrument
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.Outcome
import org.scalatest.concurrent.TimeLimits.failAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.SpanSugar._

import scala.collection.mutable

class ParserSuite extends AnyFunSuite with Matchers with TimeLimitedTests {
  import fastparse._

  val timeLimit = 300000 millis

  var currentTest: String = _
  override def withFixture(test: NoArgTest): Outcome = {
    failAfter(timeLimit) {
      this.synchronized {
        currentTest = test.name
        super.withFixture(test)
      }
    }
  }

  def p[T](parser: P[_] => P[T], x: T): Unit = parses(currentTest, parser, x)

  trait InstrumentRun {
    def before(name: String, ctx: ParsingRun[_]): Unit
    def after(name: String, ctx: ParsingRun[_]): Unit
  }

  case class Debugger() extends Instrument {
    private val depthStack = mutable.Stack[(Int,Int)]()

    private var depth = 0

    private def output(str: String) = println(str)

    override def beforeParse(parser: String, index: Int): Unit = {
      val indent = "  " * depth
      output(s"$indent+$parser")
      depth += 1
    }

    override def afterParse(parser: String, index: Int, success: Boolean): Unit = {
      depth -= 1
      val indent = "  " * depth
      output(s"$indent-$parser")
    }
  }

  def parses[T](input: String, parser: P[_] => P[T], result: T): Unit =
    parse(input, parser/*, instrument=Debugger()*/) match {
      case Parsed.Success(value, _) => value mustEqual result
      case Parsed.Failure(_, _, extra) =>
        fail(extra.trace(true).longAggregateMsg)
    }

  def fails[T](input: String, parser: P[_] => P[T], error: String): Unit =
    parse(input, parser) match {
      case Parsed.Success(value, _) => fail(s"Parser succeeded with $value")
      case f: Parsed.Failure => f.msg mustEqual(error)
    }
}
