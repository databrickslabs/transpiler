package spl

import fastparse.MultiLineWhitespace._
import fastparse._

/**
 * SPL parser and AST
 *
 * https://docs.splunk.com/Splexicon:Searchprorcessinglanguage
 * @link https://gist.github.com/ChrisYounger/520bdb1a7c8b22f5210213f83a3ab2db
 * @link https://gist.github.com/ChrisYounger/e51f9c3aba0f1ed02e5caee7d4a6128b
 * @link https://docs.splunk.com/Documentation/Splunk/8.2.1/SearchReference/Search
 * @link https://docs.splunk.com/Documentation/Splunk/8.2.1/Search/Aboutsearchlanguagesyntax
 */
object SplParser {
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
  private[spl] def bool[_:P] =
    ("true" | "t").map(_ => Bool(true)) |
      ("false" | "f").map(_ => Bool(false))

  def token[_: P]: P[String] = ("_"|"*"|"-"|"@"|letter|digit).repX(1).! // TODO: this is a hack for @15m

  def doubleQuoted[_: P]: P[String] = P( "\"" ~ (
    CharsWhile(!"\"".contains(_))
      | "\\" ~~ AnyChar
      | !"\""
    ).rep.! ~ "\"" )
  def fvalue[_:P]: P[Value] = (doubleQuoted|token).map(Value)

  def field[_:P]: P[Value] = fvalue
  def fieldAndValue[_:P]: P[FV] = (fvalue ~ "=" ~ fvalue).map(a => FV(a._1.value, a._2.value))
  def fieldAndValueList[_: P]: P[Map[String, String]] = fieldAndValue.rep map(x => x.map(y => y.field -> y.value).toMap)
  def fieldList[_: P]: P[Seq[Value]] = field.rep(sep=",")
  def filename[_: P]: P[String] = term

  def term[_: P]: P[String] = CharsWhile(!" ".contains(_)).!

  // syntax: -?/d+(?!/.)
  private[spl] def int[_: P]: P[IntValue] = ("+" | "-").?.! ~~ digit.rep(1).! map {
    case (sign, i) => IntValue(if (sign.equals("-")) -1 * i.toInt else i.toInt)
  }

  def constant[_:P]: P[Constant] = fvalue | bool | int

  private def ALL[_: P]: P[OperatorSymbol] = (Or.P | And.P | LessThan.P | GreaterThan.P
    | GreaterEquals.P | LessEquals.P | Equals.P | NotEquals.P | InList.P | Add.P | Subtract.P
    | Multiply.P | Divide.P)

  private def binaryOf[_: P](a: => P[Expr], b: => P[OperatorSymbol]): P[Expr] =
    (a ~ (b ~ a).rep).map {
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

  def fieldIn[_:P]: P[FieldIn] = token ~ "IN(" ~~ constant.rep(sep=",") ~~ ")" map FieldIn.tupled
  def call[_: P]: P[Call] = (token ~~ "(" ~~ expr.rep(sep=",") ~~ ")").map(Call.tupled)
  def argu[_: P]: P[Expr] = call | constant
  def parens[_: P]: P[Expr] = "(" ~ expr ~ ")"
  def primary[_: P]: P[Expr] = unaryOf(expr) | fieldIn | parens | argu
  def expr[_: P]: P[Expr] = binaryOf(primary, ALL)

  def impliedSearch[_: P]: P[SearchCommand] = "search".? ~ expr.rep(max=100) map(_.reduce((a, b) => Binary(a, And, b))) map SearchCommand
  def eval[_:P]: P[EvalCommand] = "eval" ~ (field ~ "=" ~ expr).rep(sep=",") map EvalCommand

  // | convert dur2sec(*delay)
  // convert (timeformat=<string>)? ( (auto|dur2sec|mstime|memk|none|num|rmunit|rmcomma|ctime|mktime) "(" <field>? ")" (as <field>)?)+
  def convert[_:P]: P[ConvertCommand] = ("convert" ~ ("timeformat=" ~ token).?
    ~ (token ~~ "(" ~ field ~ ")" ~ ("as" ~ field).?).map(FieldConversion.tupled).rep) map ConvertCommand.tupled

  def collect[_:P]: P[CollectCommand] = ("collect" ~ fieldAndValueList ~ fieldList map CollectCommand.tupled)

  // lookup <lookup-dataset> (<lookup-field> [AS <event-field>] )...
  //[ (OUTPUT | OUTPUTNEW) ( <lookup-destfield> [AS <event-destfield>] )...]
  def aliasedField[_:P]: P[Alias] = (field ~ W("AS") ~ (token|doubleQuoted) map Alias.tupled)
  def fieldRep[_:P]: P[Seq[Field]] = (aliasedField | field).filter {
    case Alias(Value(field), alias) => field.toLowerCase() != "output"
    case Value(v) => v.toLowerCase != "output"
  }.rep(1)
  def lookupOutput[_:P]: P[LookupOutput] = (W("OUTPUT")|W("OUTPUTNEW")).! ~ fieldRep map LookupOutput.tupled
  def lookup[_:P]: P[LookupCommand] = "lookup" ~ token ~ fieldRep ~ lookupOutput.? map LookupCommand.tupled

  /**
   * https://docs.splunk.com/Documentation/Splunk/8.2.1/SearchReference/Head
   * Function is missing the case where both a limit and a condition are passed
   * ie. head limit=10 (1==1)
   * TODO Add condition
   */
  def head[_:P]: P[HeadCommand] = ("head" ~ ((int | "limit=" ~ int) | expr)
                                          ~ ("keeplast=" ~ bool).?
                                          ~ ("null=" ~ bool).?).map(item => {
    HeadCommand(item._1, item._2.getOrElse(Bool(false)), item._3.getOrElse(Bool(false)))
  })

  /**
   * https://docs.splunk.com/Documentation/Splunk/8.2.1/SearchReference/Fields
   * Function is missing wildcard fields (except when discarding fields ie. fields - myField, ...)
   */
  def fields[_:P]: P[FieldsCommand] = "fields" ~ ("+" | "-").!.? ~ field.rep(min = 1, sep = ",") map FieldsCommand.tupled

  /**
   * https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Sort
   * ip
   */
  def sort[_:P]: P[SortCommand] = "sort" ~ (("+"|"-").!.? ~~ expr).rep(min = 1, sep = ",") map SortCommand
  // where <predicate-expression>
  def where[_:P]: P[WhereCommand] = "where" ~ expr map WhereCommand

  def table[_:P]: P[TableCommand] = "table" ~ field.rep(1) map TableCommand

  // https://docs.splunk.com/Documentation/SplunkCloud/8.2.2106/SearchReference/Stats
  def aliasedCall[_:P] = call ~ W("as") ~ token map Alias.tupled
  def stats[_:P] = ("stats" ~ fieldAndValueList ~
    (aliasedCall | call | token.filter(!_.toLowerCase.equals("by")).map(Call(_))).rep(1, ",".?) ~
    (W("by") ~ fieldList).?.map(fields => fields.getOrElse(Seq())) ~
    ("dedup_splitvals" ~ "=" ~ bool).?.map(v => v.exists(_.value)))
    .map(StatsCommand.tupled)

  // https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/Rex
  def rex[_:P]: P[RexCommand] = ("rex" ~ fieldAndValueList ~ doubleQuoted) map { case (kv, regex) =>
    RexCommand(
      field = kv.get("field"),
      maxMatch = kv.get("max_match").map(_.toInt).getOrElse(1),
      offsetField = kv.get("offset_field"),
      mode = kv.get("mode"),
      regex = regex)
  }

  def rename[_:P]: P[RenameCommand] = "rename" ~ aliasedField.rep(min = 1, sep = ",") map RenameCommand
  def _regex[_:P]: P[RegexCommand] = "regex" ~ (field ~ ("="|"!=").!).? ~ doubleQuoted map RegexCommand.tupled
  def join[_:P]: P[JoinCommand] = ("join" ~ ("type=" ~ ("inner"|"outer"|"left").!).?
                                          ~ ("usetime=" ~ bool).?
                                          ~ ("earlier=" ~ bool).?
                                          ~ ("overwrite=" ~ bool).?
                                          ~ ("max=" ~ int).?
                                          ~ field.rep(min = 1, sep = ",")
                                          ~ subSearch) map { command => {
      JoinCommand(
        command._1 match {
          case Some(value) => value
          case None => "inner"
        },
        command._2 match {
          case Some(useTime) => useTime.value
          case None => false
        },
        command._3 match {
          case Some(earlier) => earlier.value
          case None => true
        },
        command._4 match {
          case Some(overwrite) => overwrite.value
          case None => false
        },
        command._5 match {
          case Some(max) => max.value
          case None => 1
        },
        command._6,
        command._7
      )
    }
  }

  def _return[_:P]: P[ReturnCommand] = "return" ~ int.? ~ (
      (field ~ "=" ~ expr).rep(1) | ("$" ~~ field).rep(1) | field.rep(1)) map ReturnCommand.tupled

  def fillNull[_:P]: P[FillNullCommand] = ("fillnull" ~ ("value=" ~~ (doubleQuoted|token)).?
                                                      ~ field.rep(1).?) map FillNullCommand.tupled

  def command[_:P]: P[Command] = (stats | table
                                        | where
                                        | lookup
                                        | collect
                                        | convert
                                        | eval
                                        | head
                                        | fields
                                        | sort
                                        | rex
                                        | rename
                                        | _regex
                                        | join
                                        | _return
                                        | fillNull
                                        | impliedSearch)

  def subSearch[_:P]: P[Pipeline] = "[".? ~ (command rep(sep="|")) ~ "]".? map Pipeline
  def pipeline[_:P]: P[Pipeline] = (command rep(sep="|")) ~ End map Pipeline
}

