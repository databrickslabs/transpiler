package splunkql

import fastparse._, SingleLineWhitespace._

/**
 * SPL parser and AST
 *
 * https://docs.splunk.com/Splexicon:Searchprocessinglanguage
 * @link https://gist.github.com/ChrisYounger/520bdb1a7c8b22f5210213f83a3ab2db
 * @link https://gist.github.com/ChrisYounger/e51f9c3aba0f1ed02e5caee7d4a6128b
 * @link https://github.com/olafhartong/ThreatHunting/blob/master/default/savedsearches.conf (http://ini4j.sourceforge.net/)
 * @link https://docs.splunk.com/Documentation/Splunk/8.2.1/SearchReference/Search
 * @link https://docs.splunk.com/Documentation/Splunk/8.2.1/Search/Aboutsearchlanguagesyntax
 */

trait Expr
sealed trait Constant extends Expr
case class Null() extends Constant
case class Bool(value: Boolean) extends Constant
trait Field
case class Value(value: String) extends Constant with Field
case class FV(field: String, value: String) extends Expr
case class FvList(fvs: Seq[FV]) extends Expr
case class IntValue(value: Int) extends Constant


sealed trait OperatorSymbol {
  def P[_: P]: P[OperatorSymbol] = symbols.map(_ => this).opaque(getClass.getSimpleName)
  def symbols[_: P]: P[Unit] = toString
  def precedence: Int
}

sealed trait Straight extends OperatorSymbol
sealed trait Relational extends Straight

case object Or extends Straight {
  override def toString: String = "OR"
  override val precedence: Int = 9
}

case object And extends Straight {
  override def symbols[_: P]: P[Unit] = IgnoreCase("AND") | " "
  override def toString: String = "AND"
  override val precedence: Int = 8
}

case object LessThan extends Relational {
  override def toString: String = "<"
  override val precedence: Int = 7
}

case object GreaterThan extends Relational {
  override def toString: String = ">"
  override val precedence: Int = 7
}

case object GreaterEquals extends Relational {
  override def toString: String = ">="
  override val precedence: Int = 7
}

case object LessEquals extends Relational {
  override def toString: String = "<="
  override val precedence: Int = 7
}

case object Equals extends Relational {
  override def toString: String = "="
  override val precedence: Int = 7
}

case object InList extends Straight {
  override def symbols[_: P]: P[Unit] = " " ~~ IgnoreCase(toString)
  override def toString: String = "IN"
  override val precedence: Int = 7
}

case object NotEquals extends Relational {
  override def toString: String = "!="
  override val precedence: Int = 7
}

case object UnaryNot extends OperatorSymbol {
  override def precedence: Int = 2
  override def toString: String = "NOT"
}

case class Binary(left: Expr, symbol: OperatorSymbol, right: Expr) extends Expr
case class Unary(symbol: OperatorSymbol, right: Expr) extends Expr
case class Call(name: String, args: Seq[Expr] = Seq()) extends Expr
case class FieldIn(field: String, exprs: Seq[Expr]) extends Expr

trait Command
case class SearchCommand(expr: Expr) extends Command
case class EvalCommand(fields: Seq[(Value,Expr)]) extends Command
case class FieldConversion(func: String, field: Value, alias: Option[Value])
case class ConvertCommand(timeformat: Option[String], convs: Seq[FieldConversion]) extends Command
case class CollectCommand(args: Map[String,String], fields: Seq[Value]) extends Command
case class AliasedField(field: Value, alias: String) extends Expr with Field
case class LookupCommand(options: Map[String,String], dataset: String, fields: Seq[Field], output: Option[LookupOutput]) extends Command
case class LookupOutput(kv: String, fields: Seq[Field])
case class WhereCommand(expr: Expr) extends Command
case class TableCommand(fields: Seq[Value]) extends Command
case class Pipeline(commands: Seq[Command])

private[splunkql] object SPL {
  implicit def debugLogger[R](r: R) = new {
    def @@ (implicit ctx: P[_], name: sourcecode.Name): R = {
      if (ctx.logDepth == -1) return r
      val indent = "  " * ctx.logDepth
      val rep = ctx.successValue.toString.replaceAll("ArrayBuffer", "Seq")
      val debug = rep.substring(0, Math.min(rep.length, 128))
      if ("()".equals(debug)) return r
      print(s"$indent@${name.value}: ${debug}\n")
      r
    }

    def tap(l: R => String): R = {
      println(l(r))
      r
    }
  }

  private def letter[_: P] = P( lowercase | uppercase )
  private def lowercase[_: P] = P( CharIn("a-z") )
  private def uppercase[_: P] = P( CharIn("A-Z") )
  private def digit[_: P] = CharIn("0-9")

  def W[_: P](s: String): P[Unit] = IgnoreCase(s)
  private[splunkql] def bool[_:P] =
    ("true" | "t").map(_ => Bool(true)) |
      ("false" | "f").map(_ => Bool(false))

  def token[_: P]: P[String] = ("_"|"*"|"-"|"@"|letter|digit).repX(1).! // TODO: this is a hack for @15m
  def doubleQuoted[_: P]: P[String] = P( "\"" ~ (
    CharsWhile(!"\"".contains(_))
      | "\\" ~~ AnyChar
      | !"\""
    ).rep.! ~ "\"" )
  def fvalue[_:P] = (doubleQuoted|token).map(Value)

  def field[_:P] = fvalue
  def fieldAndValue[_:P] = (fvalue ~ "=" ~ fvalue).map(a => FV(a._1.value, a._2.value))
  def fieldAndValueList[_: P] = (fieldAndValue.rep map(x => x.map(y => y.field -> y.value).toMap))
  def fieldList[_: P] = field.rep(sep=",")
  def filename[_: P] = term

  def term[_: P] = CharsWhile(!" ".contains(_)).!

  // syntax: -?/d+(?!/.)
  private[splunkql] def int[_: P]: P[IntValue] = ("+" | "-").?.! ~~ digit.rep(1).! map {
    case (sign, i) => IntValue(if (sign.equals("-")) -1 * i.toInt else i.toInt)
  }

  def constant[_:P]: P[Constant] = fvalue | bool | int

  private def ALL[_: P]: P[OperatorSymbol] = (Or.P | And.P | LessThan.P | GreaterThan.P
    | GreaterEquals.P | LessEquals.P | Equals.P | NotEquals.P | InList.P)

  private def binaryOf[_: P](a: => P[Expr], b: => P[OperatorSymbol]): P[Expr] =
    (a ~~ (b ~ a).rep).map {
      case (expr, tuples) => climb(expr, tuples)
    }
  private def unaryOf[_: P](expr: => P[Expr]): P[Unary] = UnaryNot.P ~ expr map Unary.tupled

  private def climb(left: Expr, rights: Seq[(OperatorSymbol,Expr)], prec: Int = 100): Expr =
    rights.headOption match {
      case None => left
      case Some((sym, next)) =>
        if (sym.precedence < prec) left match {
          case Binary(first, prevSymbol, right) =>
            Binary(first, prevSymbol,
              climb(Binary(right, sym, next),
                rights.tail, sym.precedence+1))
          case _ => climb(Binary(left, sym, next),
            rights.tail, sym.precedence+1)
        } else Binary(left, sym,
          climb(next, rights.tail, sym.precedence+1))
    }

  def fieldIn[_:P] = token ~ "IN(" ~~ constant.rep(sep=",") ~~ ")" map FieldIn.tupled
  def call[_: P] = (token ~~ "(" ~~ expr.rep(sep=",") ~~ ")").map(Call.tupled)
  def argu[_: P] = (call | constant)
  def parens[_: P] = ("(" ~ expr ~ ")")
  def primary[_: P] = (unaryOf(expr) | fieldIn | parens | argu)
  def expr[_: P]: P[Expr] = binaryOf(primary, ALL)

  def impliedSearch[_: P] = expr.rep(max=100) map(_.reduce((a, b) => Binary(a, And, b))) map SearchCommand
  def eval[_:P] = "eval" ~ (field ~ "=" ~ expr).rep(sep=",") map EvalCommand

  // | convert dur2sec(*delay)
  // convert (timeformat=<string>)? ( (auto|dur2sec|mstime|memk|none|num|rmunit|rmcomma|ctime|mktime) "(" <field>? ")" (as <field>)?)+
  def convert[_:P] = ("convert" ~ ("timeformat=" ~ token).?
    ~ (token ~~ "(" ~ field ~ ")" ~ ("as" ~ field).?).map(FieldConversion.tupled).rep) map ConvertCommand.tupled

  def collect[_:P] = ("collect" ~ fieldAndValueList ~ fieldList map CollectCommand.tupled)

  // lookup <lookup-dataset> (<lookup-field> [AS <event-field>] )...
  //[ (OUTPUT | OUTPUTNEW) ( <lookup-destfield> [AS <event-destfield>] )...]
  def aliasedField[_:P] = (field ~ W("AS") ~ token map AliasedField.tupled)
  def fieldRep[_:P]: P[Seq[Field]] = (aliasedField | field).filter {
    case AliasedField(field, alias) => field.value.toLowerCase() != "output"
    case Value(v) => v.toLowerCase != "output"
  }.rep(1)
  def lookupOutput[_:P] = ((W("OUTPUT")|W("OUTPUTNEW")).! ~ fieldRep map LookupOutput.tupled)
  def lookup[_:P] = "lookup" ~ fieldAndValueList ~ token ~ fieldRep ~ lookupOutput.? map LookupCommand.tupled

  // where <predicate-expression>
  def where[_:P] = "where" ~ expr map WhereCommand

  def table[_:P] = "table" ~ field.rep(1) map TableCommand

  def command[_:P]: P[Command] = table | where | lookup | collect | convert | eval | impliedSearch
  def pipeline[_:P]: P[Pipeline] = (command rep(sep="|")) ~ End map Pipeline
}