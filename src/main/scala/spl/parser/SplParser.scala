package spl.parser

import fastparse.MultiLineWhitespace._
import fastparse._
import spl.ast._

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
  private[spl] def bool[_: P] =
    ("true" | "t").map(_ => Bool(true)) |
      ("false" | "f").map(_ => Bool(false))

  // TODO: add interval parsing for: us | ms | cs | ds
  def seconds[_: P]: P[String] = ("seconds" | "second" | "secs" | "sec" | "s").map(_ => "seconds")
  def minutes[_: P]: P[String] = ("minutes" | "minute" | "mins" | "min" | "m").map(_ => "minutes")
  def hours[_: P]: P[String] = ("hours" | "hour" | "hrs" | "hr" | "h").map(_ => "hours")
  def days[_: P]: P[String] = ("days" | "day" | "d").map(_ => "days")
  def weeks[_: P]: P[String] = ("weeks" | "week" | "w7" | "w0" | "w").map(_ => "weeks")
  def months[_: P]: P[String] = ("months" | "month" | "mon").map(_ => "months")
  def timeUnit[_: P]: P[String] = months|days|hours|minutes|weeks|seconds
  def timeSpan[_: P]: P[TimeSpan] = int ~~ timeUnit map {
    case (IntValue(v), interval) => TimeSpan(v, interval)
  }
  def timeSpanOne[_: P]: P[TimeSpan] = "-".!.? ~~ timeUnit map {
    case (Some("-"), interval) => TimeSpan(-1, interval)
    case (None, interval) => TimeSpan(1, interval)
    case a: Any => throw new IllegalArgumentException(s"timeSpan $a")
  }
  // https://docs.splunk.com/Documentation/SCS/current/Search/Specifyrelativetime
  def relativeTime[_: P]: P[SnapTime] = ((timeSpan|timeSpanOne).? ~~ "@" ~~ timeUnit ~~ timeSpan.?).map(SnapTime.tupled)

  def token[_: P]: P[String] = ("_"|"*"|letter|digit).repX(1).!
  def doubleQuoted[_: P]: P[String] = P( "\"" ~ (
    CharsWhile(!"\"".contains(_))
      | "\\" ~~ AnyChar
      | !"\""
    ).rep.! ~ "\"" )

  def wildcard[_: P]: P[Wildcard] = (doubleQuoted.filter(_.contains("*")) | token.filter(_.contains("*"))) map Wildcard
  def strValue[_: P]: P[StrValue] = doubleQuoted map StrValue
  def field[_: P]: P[Field] = token.filter(!Seq("t", "f").contains(_)) map Field
  def byte[_: P]: P[String] = digit.rep(min=1, max=3).!
  def cidr[_: P]: P[IPv4CIDR] = (byte.rep(sep=".", exactly=4) ~ "/" ~ byte).! map IPv4CIDR
  def fieldAndValue[_: P]: P[FV] = (token ~ "=" ~ (doubleQuoted|token)) map { case (k, v) => FV(k, v) }
  def fieldAndConstant[_: P]: P[FC] = (token ~ "=" ~ constant) map { case (k, v) => FC(k, v) }
  def commandOptions[_: P]: P[CommandOptions] = fieldAndConstant.rep map CommandOptions
  @deprecated("use commandOptions", "0.2") def fieldAndValueList[_: P]: P[Map[String, String]] = fieldAndValue.rep map(x => x.map(y => y.field -> y.value).toMap)
  @deprecated("use commandOptions", "0.2") def fieldAndBoolList[_: P]: P[Map[String, Boolean]] = fieldAndBool.rep map(x => x.map(y => y.field -> y.value).toMap)
  @deprecated("use commandOptions", "0.2") def fieldAndBool[_: P]: P[FB] = (token ~ "=" ~ bool).map { case (k, v) => FB(k, v.value)}

  def fieldList[_: P]: P[Seq[Field]] = field.rep(sep=",")
  def filename[_: P]: P[String] = term

  def term[_: P]: P[String] = CharsWhile(!" ".contains(_)).!

  // syntax: -?/d+(?!/.)
  private[spl] def int[_: P]: P[IntValue] = ("+" | "-").?.! ~~ digit.rep(1).! map {
    case (sign, i) => IntValue(if (sign.equals("-")) -1 * i.toInt else i.toInt)
  }

  private[spl] def double[_: P]: P[DoubleValue] = ("+" | "-").?.! ~~ digit.rep(1).! ~~ "." ~~ digit.rep(1).! map {
    case (sign, i, j) => DoubleValue(if (sign.equals("-")) -1 * (i + "." + j).toDouble else (i + "." + j).toDouble)
  }

  def constant[_: P]: P[Constant] = cidr | wildcard | strValue | relativeTime | timeSpan | double | int | field | bool

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

  def fieldIn[_: P]: P[FieldIn] = token ~ "IN" ~ "(" ~ constant.rep(sep=",".?) ~ ")" map FieldIn.tupled
  def call[_: P]: P[Call] = (token ~~ "(" ~~ expr.rep(sep=",") ~~ ")").map(Call.tupled)

  def termCall[_: P]: P[Call] = (W("TERM") ~ "(" ~ CharsWhile(!")".contains(_)).! ~ ")").map(term => Call("TERM", Seq(Field(term))))
  def argu[_: P]: P[Expr] = termCall | call | constant
  def parens[_: P]: P[Expr] = "(" ~ expr ~ ")"
  def primary[_: P]: P[Expr] = unaryOf(expr) | fieldIn | parens | argu
  def expr[_: P]: P[Expr] = binaryOf(primary, ALL)

  def impliedSearch[_: P]: P[SearchCommand] = "search".? ~ expr.rep(max=100) map(_.reduce((a, b) => Binary(a, And, b))) map SearchCommand
  def eval[_: P]: P[EvalCommand] = "eval" ~ (field ~ "=" ~ expr).rep(sep=",") map EvalCommand

  // | convert dur2sec(*delay)
  // convert (timeformat=<string>)? ( (auto|dur2sec|mstime|memk|none|num|rmunit|rmcomma|ctime|mktime) "(" <field>? ")" (as <field>)?)+
  def convert[_: P]: P[ConvertCommand] = ("convert" ~ ("timeformat=" ~ token).?
    ~ (token ~~ "(" ~ field ~ ")" ~ ("as" ~ field).?).map(FieldConversion.tupled).rep) map ConvertCommand.tupled

  def collect[_: P]: P[CollectCommand] = ("collect" ~ fieldAndValueList ~ fieldList map CollectCommand.tupled)

  // lookup <lookup-dataset> (<lookup-field> [AS <event-field>] )...
  // [ (OUTPUT | OUTPUTNEW) ( <lookup-destfield> [AS <event-destfield>] )...]
  def aliasedField[_: P]: P[Alias] = (field ~ W("AS") ~ (token|doubleQuoted) map Alias.tupled)
  def fieldRep[_: P]: P[Seq[FieldLike]] = (aliasedField | field).filter {
    case Alias(Field(field), _) => field.toLowerCase() != "output"
    case Field(v) => v.toLowerCase != "output"
    case _ => false
  }.rep(1)
  def lookupOutput[_: P]: P[LookupOutput] = (W("OUTPUT")|W("OUTPUTNEW")).! ~ fieldRep map LookupOutput.tupled
  def lookup[_: P]: P[LookupCommand] = "lookup" ~ token ~ fieldRep ~ lookupOutput.? map LookupCommand.tupled

  /**
   * https://docs.splunk.com/Documentation/Splunk/8.2.1/SearchReference/Head
   * Function is missing the case where both a limit and a condition are passed
   * ie. head limit=10 (1==1)
   * TODO Add condition
   * TODO: refactor to use command options
   */
  def head[_: P]: P[HeadCommand] = ("head" ~ ((int | "limit=" ~ int) | expr)
    ~ ("keeplast=" ~ bool).?
    ~ ("null=" ~ bool).?).map(item => {
    HeadCommand(item._1, item._2.getOrElse(Bool(false)), item._3.getOrElse(Bool(false)))
  })

  /**
   * https://docs.splunk.com/Documentation/Splunk/8.2.1/SearchReference/Fields
   * Function is missing wildcard fields (except when discarding fields ie. fields - myField, ...)
   */
  def fields[_: P]: P[FieldsCommand] = "fields" ~ ("+" | "-").!.? ~ field.rep(min = 1, sep = ",") map {
    case (op, fields) =>
      if (op.getOrElse("+").equals("-"))
        FieldsCommand(removeFields = true, fields)
      else
        FieldsCommand(removeFields = false, fields)
  }

  /**
   * https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Sort
   */
  def sort[_: P]: P[SortCommand] = "sort" ~ (("+"|"-").!.? ~~ expr).rep(min = 1, sep = ",") map SortCommand
  // where <predicate-expression>
  def where[_: P]: P[WhereCommand] = "where" ~ expr map WhereCommand

  def table[_: P]: P[TableCommand] = "table" ~ field.rep(1) map TableCommand

  // https://docs.splunk.com/Documentation/SplunkCloud/8.2.2106/SearchReference/Stats
  def aliasedCall[_: P] = call ~ W("as") ~ token map Alias.tupled
  def statsCall[_: P] = (aliasedCall | call |
    token.filter(!_.toLowerCase.equals("by")).map(Call(_))).rep(1, ",".?)

  def stats[_: P] = ("stats" ~ fieldAndValueList ~ statsCall ~
    (W("by") ~ fieldList).?.map(fields => fields.getOrElse(Seq())) ~
    ("dedup_splitvals" ~ "=" ~ bool).?.map(v => v.exists(_.value)))
    .map(StatsCommand.tupled)

  // https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/Rex
  def rex[_: P]: P[RexCommand] = ("rex" ~ commandOptions ~ doubleQuoted) map {
    case (kv, regex) =>
      RexCommand(
        field = kv.getStringOption("field"),
        maxMatch = kv.getInt("max_match", 1),
        offsetField = kv.getStringOption("offset_field"),
        mode = kv.getStringOption("mode"),
        regex = regex)
  }

  def rename[_: P]: P[RenameCommand] = "rename" ~ aliasedField.rep(min = 1, sep = ",") map RenameCommand
  def _regex[_: P]: P[RegexCommand] = "regex" ~ (field ~ ("="|"!=").!).? ~ doubleQuoted map RegexCommand.tupled
  def join[_: P]: P[JoinCommand] = ("join" ~ commandOptions ~ field.rep(min = 1, sep = ",") ~ subSearch) map {
    case (options, fields, pipeline) => JoinCommand(
      joinType = options.getString("type", "inner"),
      useTime = options.getBoolean("usetime"),
      earlier = options.getBoolean("earlier", default = true),
      overwrite = options.getBoolean("overwrite"),
      max = options.getInt("max", 1),
      fields = fields,
      subSearch = pipeline)
  }

  def _return[_: P]: P[ReturnCommand] = "return" ~ int.? ~ (
    fieldAndValue.rep(1) | ("$" ~~ field).rep(1) | field.rep(1)) map {
    case (maybeValue, exprs) =>
      ReturnCommand(maybeValue.getOrElse(IntValue(1)), exprs map {
        case fv: FV => Alias(Field(fv.value), fv.field).asInstanceOf[FieldOrAlias]
        case field: Field => field.asInstanceOf[FieldOrAlias]
        case a: Any => throw new IllegalArgumentException(s"field $a")
      })
  }

  def fillNull[_: P]: P[FillNullCommand] = ("fillnull" ~ ("value=" ~~ (doubleQuoted|token)).?
    ~ field.rep(1).?) map FillNullCommand.tupled

  def eventStats[_: P]: P[EventStatsCommand] = ("eventstats" ~ fieldAndValueList ~ statsCall
    ~ (W("by") ~ fieldList).?.map(fields => fields.getOrElse(Seq()))).map(EventStatsCommand.tupled)

  def streamStats[_: P]: P[StreamStatsCommand] = ("streamstats" ~ commandOptions ~ statsCall
    ~ (W("by") ~ fieldList).?.map(fields => fields.getOrElse(Seq()))).map {
    case (options, funcs, by) =>
      StreamStatsCommand(
        funcs,
        by,
        options.getBoolean("current", true),
        options.getInt("window", 0)
      )
  }

  /**
   * Specific field repetition which exclude the term sortby
   * to avoid any conflict with the sortby command during the parsing
   */
  def dedupFieldRep[_: P]: P[Seq[Field]] =  field.filter {
    case Field(myVal) => !myVal.toLowerCase.equals("sortby")
  }.rep(1)

  def dedup[_: P]: P[DedupCommand] = (
    "dedup" ~ int.? ~ commandOptions ~ dedupFieldRep
      ~ ("sortby" ~ (("+"|"-").!.? ~~ field).rep(1)).?) map {
    case (limit, kv, fields, sortByQuery) =>
      val sortByCommand = sortByQuery match {
        case Some(query) => SortCommand(query)
        case _ => SortCommand(Seq((Some("+"), Field("_no"))))
      }
      DedupCommand(
        numResults = limit.getOrElse(IntValue(1)).value,
        fields = fields,
        keepEvents = kv.getBoolean("keepevents"),
        keepEmpty = kv.getBoolean("keepEmpty"),
        consecutive = kv.getBoolean("consecutive"),
        sortByCommand
      )
  }

  def inputLookup[_: P]: P[InputLookup] = ("inputlookup" ~ commandOptions ~ token ~ ("where" ~ expr).?) map {
    case (options, tableName, whereOption) =>
      InputLookup(
        options.getBoolean("append"),
        options.getBoolean("strict"),
        options.getInt("start", 0),
        options.getInt("max", 1000000000),
        tableName,
        whereOption)
  }

  def format[_: P]: P[FormatCommand] = ("format" ~ commandOptions ~ doubleQuoted.rep(6).?) map {
    case (kv, options) =>
      val arguments = options match {
        case Some(args) => args
        case _ => Seq("(", "(", "AND", ")", "OR", ")")
      }
      FormatCommand(
        mvSep = kv.getString("mvsep", "OR"),
        maxResults = kv.getInt("maxresults"),
        rowPrefix = arguments.head,
        colPrefix = arguments(1),
        colSep = arguments(2),
        colEnd = arguments(3),
        rowSep = arguments(4),
        rowEnd = arguments(5)
      )
  }

  def mvcombine[_: P]: P[MvCombineCommand] = ("mvcombine" ~ ("delim" ~ "=" ~ doubleQuoted).?
    ~ field) map MvCombineCommand.tupled

  def mvexpand[_: P]: P[MvExpandCommand] = ("mvexpand" ~ field ~ ("limit" ~ "=" ~ int).?) map {
    case (field, None) => MvExpandCommand(field, None)
    case (field, Some(limit)) => MvExpandCommand(field, Some(limit.value))
  }

  // bin [<bin-options>...] <field> [AS <newfield>]
  def bin[_: P]: P[BinCommand] = "bin" ~ commandOptions ~ (aliasedField | field) map {
    case (options, field) => BinCommand(field,
      span = options.getSpanOption("span"),
      minSpan = options.getSpanOption("minspan"),
      bins = options.getIntOption("bins"),
      start = options.getIntOption("start"),
      end = options.getIntOption("end"),
      alignTime = options.getStringOption("aligntime")
    )
  }

  def makeResults[_: P]: P[MakeResults] = ("makeresults" ~ commandOptions) map {
    options =>
      MakeResults(
        count = options.getInt("count", 1),
        annotate = options.getBoolean("annotate"),
        splunkServer = options.getString("splunk_server", "local"),
        splunkServerGroup = options.getString("splunk_server_group", null)
      )
  }

  def addTotals[_: P]: P[AddTotals] = "addtotals" ~ commandOptions ~ field.rep(1).? map {
    case (options: CommandOptions, fields: Option[Seq[Field]]) =>
      AddTotals(
        fields.getOrElse(Seq.empty[Field]),
        row = options.getBoolean("row", default = true),
        col = options.getBoolean("col"),
        fieldName = options.getString("fieldname", "Total"),
        labelField = options.getString("labelfield", null),
        label = options.getString("label", "Total")
      )
  }

  def command[_: P]: P[Command] = (stats
    | table
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
    | streamStats
    | dedup
    | inputLookup
    | format
    | mvcombine
    | mvexpand
    | bin
    | makeResults
    | addTotals
    | impliedSearch)

  def subSearch[_: P]: P[Pipeline] = "[".? ~ (command rep(sep="|")) ~ "]".? map Pipeline
  def pipeline[_: P]: P[Pipeline] = (command rep(sep="|")) ~ End map Pipeline
}