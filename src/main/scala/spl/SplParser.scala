package spl

import fastparse.MultiLineWhitespace._
import fastparse._

import scala.util.Try

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

  // TODO: this is a hack for earliest=-15m@m
  def token[_: P]: P[String] = ("_"|"*"|"-"|"@"|letter|digit).repX(1).!
  def doubleQuoted[_: P]: P[String] = P( "\"" ~ (
    CharsWhile(!"\"".contains(_))
      | "\\" ~~ AnyChar
      | !"\""
    ).rep.! ~ "\"" )

  def wildcard[_:P]: P[Wildcard] = (doubleQuoted.filter(_.contains("*")) | token.filter(_.contains("*"))) map Wildcard
  def strValue[_:P]: P[StrValue] = doubleQuoted map StrValue
  def field[_:P]: P[Field] = token.filter(!Seq("t", "f").contains(_)) map Field
  def byte[_:P]: P[String] = digit.rep(min=1, max=3).!
  def cidr[_:P]: P[IPv4CIDR] = (byte.rep(sep=".", exactly=4) ~ "/" ~ byte).! map IPv4CIDR

  def fieldAndValue[_:P]: P[FV] = (token ~ "=" ~ (doubleQuoted|token)) map { case (k, v) => FV(k, v) }
  def fieldAndValueList[_: P]: P[Map[String, String]] = fieldAndValue.rep map(x => x.map(y => y.field -> y.value).toMap)
  def fieldAndBool[_:P]: P[FB] = (token ~ "=" ~ bool).map { case (k, v) => FB(k, v.value)}
  def fieldAndBoolList[_: P]: P[Map[String, Boolean]] = fieldAndBool.rep map(x => x.map(y => y.field -> y.value).toMap)
  def fieldList[_: P]: P[Seq[Field]] = field.rep(sep=",")
  def filename[_: P]: P[String] = term

  def term[_: P]: P[String] = CharsWhile(!" ".contains(_)).!

  // syntax: -?/d+(?!/.)
  private[spl] def int[_: P]: P[IntValue] = ("+" | "-").?.! ~~ digit.rep(1).! map {
    case (sign, i) => IntValue(if (sign.equals("-")) -1 * i.toInt else i.toInt)
  }

  def constant[_:P]: P[Constant] = cidr | wildcard | strValue | int | field | bool

  private def ALL[_: P]: P[OperatorSymbol] = (Or.P | And.P | LessThan.P | GreaterThan.P
    | GreaterEquals.P | LessEquals.P | Equals.P | NotEquals.P | InList.P | Add.P | Subtract.P
    | Multiply.P | Divide.P | Concatenate.P)

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

  def fieldIn[_:P]: P[FieldIn] = token ~ "IN" ~ "(" ~ constant.rep(sep=",".?) ~ ")" map FieldIn.tupled
  def call[_: P]: P[Call] = (token ~~ "(" ~~ expr.rep(sep=",") ~~ ")").map(Call.tupled)

  def termCall[_: P]: P[Call] = (W("TERM") ~ "(" ~ CharsWhile(!")".contains(_)).! ~ ")").map(term => Call("TERM", Seq(Field(term))))
  def argu[_: P]: P[Expr] = termCall | call | constant
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
  def fieldRep[_:P]: P[Seq[FieldLike]] = (aliasedField | field).filter {
    case Alias(Field(field), alias) => field.toLowerCase() != "output"
    case Field(v) => v.toLowerCase != "output"
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
  def fields[_:P]: P[FieldsCommand] = "fields" ~ ("+" | "-").!.? ~ field.rep(min = 1, sep = ",") map {
    case (op, fields) =>
      if (op.getOrElse("+").equals("-"))
        FieldsCommand(removeFields = true, fields)
      else
        FieldsCommand(removeFields = false, fields)
  }

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
  def statsCall[_:P] = (aliasedCall | call |
      token.filter(!_.toLowerCase.equals("by")).map(Call(_))).rep(1, ",".?)

  def stats[_:P] = ("stats" ~ fieldAndValueList ~ statsCall ~
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
      (fieldAndValue).rep(1) | ("$" ~~ field).rep(1) | field.rep(1)) map {
    case (maybeValue, exprs) =>
      ReturnCommand(maybeValue.getOrElse(IntValue(1)), exprs map {
        case fv: FV => Alias(Field(fv.value), fv.field).asInstanceOf[FieldOrAlias]
        case field: Field => field.asInstanceOf[FieldOrAlias]
      })
  }

  def fillNull[_:P]: P[FillNullCommand] = ("fillnull" ~ ("value=" ~~ (doubleQuoted|token)).?
                                                      ~ field.rep(1).?) map FillNullCommand.tupled

  def eventStats[_:P]: P[EventStatsCommand] = ("eventstats" ~ fieldAndValueList ~ statsCall
      ~ (W("by") ~ fieldList).?.map(fields => fields.getOrElse(Seq()))).map(EventStatsCommand.tupled)

  /**
   * Specific field repetition which exclude the term sortby
   * to avoid any conflict with the sortby command during the parsing
   */
  def dedupFieldRep[_:P]: P[Seq[Field]] =  field.filter {
    case Field(myVal) => !myVal.toLowerCase.equals("sortby")
  }.rep(1)

  def dedup[_:P]: P[DedupCommand] = (
      "dedup" ~ int.? ~ fieldAndBoolList.? ~ dedupFieldRep
              ~ ("sortby" ~ (("+"|"-").!.? ~~ field).rep(1)).?) map {
    case (limit, kv, fields, sortByQuery) =>
      val kvOpt = kv.getOrElse(Map[String, Boolean]())
      val sortByCommand = sortByQuery match {
        case Some(query) => SortCommand(query)
        case _ => SortCommand(Seq((Some("+"), spl.Field("_no"))))
      }

      DedupCommand(
        numResults = (limit.getOrElse(IntValue(1))).value,
        fields = fields,
        keepEvents = kvOpt.getOrElse("keepevents", false),
        keepEmpty = kvOpt.getOrElse("keepEmpty", false),
        consecutive = kvOpt.getOrElse("consecutive", false),
        sortByCommand
      )
  }

  private def toBool(str: String): Boolean = {
    str.toLowerCase match {
      case "true" => true
      case "t" => true
      case "false" => false
      case "f" => false
      case _ => false
    }
  }

  def inputLookup[_:P]: P[InputLookup] = ("inputlookup" ~ fieldAndValueList.? ~ token ~ ("where" ~ expr).?) map {
    case (kv, tableName, whereOption) =>
      val kvOpt: Map[String, String] = kv.getOrElse(Map[String, String]())
      InputLookup(
        kvOpt.get("append").map(toBool).getOrElse(false),
        kvOpt.get("strict").map(toBool).getOrElse(false),
        kvOpt.get("start").map(_.toInt).getOrElse(0),
        kvOpt.get("max").map(_.toInt).getOrElse(1000000000),
        tableName,
        whereOption
      )
  }

  def format[_:P]: P[FormatCommand] = ("format" ~ fieldAndValueList.? ~ doubleQuoted.rep(6).?) map {
    case (kv, options) =>
      val kvOpt: Map[String, String] = kv.getOrElse(Map[String, String]())
      val arguments = options match {
        case Some(args) => args
        case _ => Seq("(", "(", "AND", ")", "OR", ")")
      }

      FormatCommand(
        mvSep = kvOpt.getOrElse("mvsep", "OR"),
        maxResults = kvOpt.get("maxresults").map(_.toInt).getOrElse(0),
        rowPrefix = arguments.head,
        colPrefix = arguments(1),
        colSep = arguments(2),
        colEnd = arguments(3),
        rowSep = arguments(4),
        rowEnd = arguments(5)
      )
  }

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
                                        | eventStats
                                        | dedup
                                        | inputLookup
                                        | format
                                        | impliedSearch)

  def subSearch[_:P]: P[Pipeline] = "[".? ~ (command rep(sep="|")) ~ "]".? map Pipeline
  def pipeline[_:P]: P[Pipeline] = (command rep(sep="|")) ~ End map Pipeline
}

