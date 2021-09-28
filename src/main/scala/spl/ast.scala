package spl

abstract sealed class Expr
abstract sealed class LeafExpr extends Expr

sealed trait Field
abstract sealed class Constant extends LeafExpr

case class Null() extends Constant
case class Bool(value: Boolean) extends Constant
case class IntValue(value: Int) extends Constant
case class Value(value: String) extends Constant with Field
case class FV(field: String, value: String) extends LeafExpr

case class AliasedField(field: Value, alias: String) extends Expr with Field

case class FvList(fvs: Seq[FV]) extends Expr

case class Binary(left: Expr, symbol: OperatorSymbol, right: Expr) extends Expr

case class Unary(symbol: OperatorSymbol, right: Expr) extends Expr

case class Call(name: String, args: Seq[Expr] = Seq()) extends Expr

case class FieldIn(field: String, exprs: Seq[Expr]) extends Expr

case class Alias(expr: Expr, name: String) extends Expr with Field

abstract sealed class Command

case class SearchCommand(expr: Expr) extends Command

case class EvalCommand(fields: Seq[(Value,Expr)]) extends Command

case class FieldConversion(func: String, field: Value, alias: Option[Value])

case class ConvertCommand(timeformat: Option[String], convs: Seq[FieldConversion]) extends Command

case class LookupOutput(kv: String, fields: Seq[Field])

case class LookupCommand(dataset: String, fields: Seq[Field], output: Option[LookupOutput]) extends Command {
  /** Does this lookup command have any aliases from the right side of the join? */
  def hasOutput: Boolean = output.isDefined
}

case class CollectCommand(args: Map[String,String], fields: Seq[Value]) extends Command

case class WhereCommand(expr: Expr) extends Command

case class TableCommand(fields: Seq[Value]) extends Command

case class HeadCommand(evalExpr: Expr,
                       keepLast: Bool = Bool(false),
                       nullOption: Bool = Bool(false)) extends Command

case class FieldsCommand(op: Option[String], fields: Seq[Value]) extends Command

case class SortCommand(fieldsToSort: Seq[(Option[String], Expr)]) extends Command

case class StatsCommand(params: Map[String, String],
                        funcs: Seq[Expr],
                        by: Seq[Value] = Seq(),
                        dedupSplitVals: Boolean = false) extends Command

case class RexCommand(field: Option[String],
                      maxMatch: Int,
                      offsetField: Option[String],
                      mode: Option[String],
                      regex: String) extends Command

case class RenameCommand(alias: Seq[Alias]) extends Command

case class RegexCommand(item: Option[(Value, String)], regex: String) extends Command

case class JoinCommand(joinType: String = "inner",
                       useTime: Boolean = false,
                       earlier: Boolean = true,
                       overwrite: Boolean = true,
                       max: Int = 1,
                       fields: Seq[Value],
                       subSearch: Pipeline) extends Command

// TODO: replace "Product with Serializable" with "Field" and refactor things
case class ReturnCommand(count: Option[IntValue], fields: Seq[Product with Serializable]) extends Command

// TODO: Option[Seq[Value]] -> Seq[Value] = Seq()
case class FillNullCommand(value: Option[String], fields: Option[Seq[Value]]) extends Command

case class Pipeline(commands: Seq[Command])