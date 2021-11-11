package spl.ast

sealed trait Expr
sealed trait LeafExpr extends Expr

sealed trait FieldLike
sealed trait Constant extends LeafExpr
sealed trait SplSpan extends Constant
sealed trait FieldOrAlias

case class Null() extends Constant
case class Bool(value: Boolean) extends Constant
case class IntValue(value: Int) extends Constant
case class DoubleValue(value: Double) extends Constant
case class StrValue(value: String) extends Constant
case class TimeSpan(value: Int, scale: String) extends SplSpan
case class SnapTime(span: Option[TimeSpan], snap: String,
                    snapOffset: Option[TimeSpan]) extends Constant
case class Field(value: String) extends Constant with FieldLike with FieldOrAlias
case class Wildcard(value: String) extends Constant with FieldLike
case class Variable(value: String) extends Constant
case class IPv4CIDR(value: String) extends Constant

case class FV(field: String, value: String) extends LeafExpr
case class FB(field: String, value: Boolean) extends LeafExpr
case class FC(field: String, value: Constant) extends LeafExpr

case class CommandOptions(options: Seq[FC]) {
  private val inner = options.map(y => y.field -> y.value).toMap

  private def throwIAE(msg: String) = throw new IllegalArgumentException(msg)

  def toMap: Map[String, Constant] = inner
  def getIntOption(key: String): Option[Int] = inner.get(key) map {
    case IntValue(value) => value
    case other: Constant => throwIAE(s"not an int: $other")
  }

  def getInt(key: String, default: Int = 0): Int =
    getIntOption(key).getOrElse(default)

  def getStringOption(key: String): Option[String] = inner.get(key) map {
    case Field(v) => v
    case StrValue(v) => v
    case other: Constant => throwIAE(s"not a string: $other")
  }

  def getString(key: String, default: String): String =
    getStringOption(key).getOrElse(default)

  def getSpanOption(key: String): Option[SplSpan] = inner.get(key) map {
    case span: SplSpan => span
    case other: Constant => throwIAE(s"not a span: $other")
  }

  def getBoolean(key: String, default: Boolean = false): Boolean = inner.get(key) map {
    case Bool(value) => value
    case Field("true") => true
    case Field("t") => true
    case Field("false") => false
    case Field("f") => false
    case other: Constant => throwIAE(s"not a bool: $other")
  } getOrElse default
}

case class AliasedField(field: Field, alias: String) extends Expr with FieldLike

case class Binary(left: Expr, symbol: OperatorSymbol, right: Expr) extends Expr

case class Unary(symbol: OperatorSymbol, right: Expr) extends Expr

case class Call(name: String, args: Seq[Expr] = Seq()) extends Expr

case class FieldIn(field: String, exprs: Seq[Expr]) extends Expr

case class Alias(expr: Expr, name: String) extends Expr with FieldLike with FieldOrAlias

sealed trait Command

case class SearchCommand(expr: Expr) extends Command

case class EvalCommand(fields: Seq[(Field, Expr)]) extends Command

case class FieldConversion(func: String, field: Field, alias: Option[Field]) extends Expr

case class ConvertCommand(timeformat: String = "%m/%d/%Y %H:%M:%S",
                          convs: Seq[FieldConversion]) extends Command

case class LookupOutput(kv: String, fields: Seq[FieldLike])

case class LookupCommand(dataset: String, fields: Seq[FieldLike],
                         output: Option[LookupOutput]) extends Command

case class CollectCommand(index: String,
                          fields: Seq[Field],
                          addTime: Boolean,
                          file: String,
                          host: String,
                          marker: String,
                          outputFormat: String,
                          runInPreview: Boolean,
                          spool: Boolean,
                          source: String,
                          sourceType: String,
                          testMode: Boolean) extends Command

case class WhereCommand(expr: Expr) extends Command

case class TableCommand(fields: Seq[Field]) extends Command

case class HeadCommand(evalExpr: Expr,
                       keepLast: Bool = Bool(false),
                       nullOption: Bool = Bool(false)) extends Command

case class FieldsCommand(removeFields: Boolean, fields: Seq[Field]) extends Command

case class SortCommand(fieldsToSort: Seq[(Option[String], Expr)]) extends Command

case class StatsCommand(partitions: Int,
                        allNum: Boolean,
                        delim: String,
                        funcs: Seq[Expr],
                        by: Seq[Field] = Seq(),
                        dedupSplitVals: Boolean = false) extends Command

case class TStatsCommand(append: Boolean = false,
                         fillNullValue: Option[Int] = None,
                         prestats: Boolean = false,
                         funcs: Seq[Expr],
                         from: Option[String],
                         where: Option[Expr],
                         by: Seq[Field] = Seq(),
                         span: Option[TimeSpan]
                        ) extends Command

case class RexCommand(field: Option[String],
                      maxMatch: Int,
                      offsetField: Option[String],
                      mode: Option[String],
                      regex: String) extends Command

case class RenameCommand(alias: Seq[Alias]) extends Command

case class RegexCommand(item: Option[(Field, String)], regex: String) extends Command

case class JoinCommand(joinType: String = "inner",
                       useTime: Boolean = false,
                       earlier: Boolean = true,
                       overwrite: Boolean = true,
                       max: Int = 1,
                       fields: Seq[Field],
                       subSearch: Pipeline) extends Command

case class ReturnCommand(count: IntValue, fields: Seq[FieldOrAlias]) extends Command

// TODO: Option[Seq[Value]] -> Seq[Value] = Seq()
case class FillNullCommand(value: Option[String], fields: Option[Seq[Field]]) extends Command

case class EventStatsCommand(allNum: Boolean, funcs: Seq[Expr],
                             by: Seq[Field] = Seq()) extends Command

case class StreamStatsCommand(funcs: Seq[Expr], by: Seq[Field] = Seq(), current: Boolean = true,
                              window: Int = 0) extends Command

case class DedupCommand(numResults: Int, fields: Seq[Field], keepEvents: Boolean,
                        keepEmpty: Boolean, consecutive: Boolean,
                        sortBy: SortCommand) extends Command

case class InputLookup(append: Boolean, strict: Boolean, start: Int, max: Int, tableName: String,
                       where: Option[Expr]) extends Command

case class FormatArgs(rowPrefix: String, colPrefix: String, colSep: String, colEnd: String,
                      rowSep: String, rowEnd: String)

case class FormatCommand(mvSep: String, maxResults: Int, rowPrefix: String, colPrefix: String,
                         colSep: String, colEnd: String, rowSep: String,
                         rowEnd: String) extends Command

case class MvCombineCommand(delim: Option[String], field: Field) extends Command

case class MvExpandCommand(field: Field, limit: Option[Int]) extends Command

case class MakeResults(count: Int, annotate: Boolean, splunkServer: String,
                       splunkServerGroup: String) extends Command

case class AddTotals(fields: Seq[Field],
                     row: Boolean,
                     col: Boolean,
                     fieldName: String,
                     labelField: String,
                     label: String) extends Command

case class BinCommand(field: FieldOrAlias, span: Option[SplSpan] = None,
                      minSpan: Option[SplSpan] = None, bins: Option[Int] = None,
                      start: Option[Int] = None, end: Option[Int] = None,
                      alignTime: Option[String] = None) extends Command

case class MultiSearch(pipelines: Seq[Pipeline]) extends Command

case class MapCommand(search: Pipeline, maxSearches: Int) extends Command

case class Pipeline(commands: Seq[Command])
