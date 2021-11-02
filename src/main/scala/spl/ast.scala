package spl

import org.apache.commons.net.util.SubnetUtils

sealed trait Expr
sealed trait LeafExpr extends Expr

sealed trait FieldLike
sealed trait Constant extends LeafExpr

trait FieldOrAlias

case class Null() extends Constant
case class Bool(value: Boolean) extends Constant
case class IntValue(value: Int) extends Constant
case class StrValue(value: String) extends Constant
case class Field(value: String) extends Constant with FieldLike with FieldOrAlias
case class Wildcard(value: String) extends Constant with FieldLike
case class IPv4CIDR(value: String) extends Constant {
  private val subnet = new SubnetUtils(value)
  def low: String = subnet.getInfo.getLowAddress
  def high: String = subnet.getInfo.getHighAddress
}

case class FV(field: String, value: String) extends LeafExpr

case class FB(field: String, value: Boolean) extends LeafExpr

case class AliasedField(field: Field, alias: String) extends Expr with FieldLike

case class FvList(fvs: Seq[FV]) extends Expr

case class Binary(left: Expr, symbol: OperatorSymbol, right: Expr) extends Expr

case class Unary(symbol: OperatorSymbol, right: Expr) extends Expr

case class Call(name: String, args: Seq[Expr] = Seq()) extends Expr

case class FieldIn(field: String, exprs: Seq[Expr]) extends Expr

case class Alias(expr: Expr, name: String) extends Expr with FieldLike with FieldOrAlias

sealed trait Command

case class SearchCommand(expr: Expr) extends Command

case class EvalCommand(fields: Seq[(Field,Expr)]) extends Command

case class FieldConversion(func: String, field: Field, alias: Option[Field])

case class ConvertCommand(timeformat: Option[String], convs: Seq[FieldConversion]) extends Command

case class LookupOutput(kv: String, fields: Seq[FieldLike])

case class LookupCommand(dataset: String, fields: Seq[FieldLike], output: Option[LookupOutput]) extends Command {
  /** Does this lookup command have any aliases from the right side of the join? */
  def hasOutput: Boolean = output.isDefined
}

case class CollectCommand(args: Map[String,String], fields: Seq[Field]) extends Command

case class WhereCommand(expr: Expr) extends Command

case class TableCommand(fields: Seq[Field]) extends Command

case class HeadCommand(evalExpr: Expr,
                       keepLast: Bool = Bool(false),
                       nullOption: Bool = Bool(false)) extends Command

case class FieldsCommand(removeFields: Boolean, fields: Seq[Field]) extends Command

case class SortCommand(fieldsToSort: Seq[(Option[String], Expr)]) extends Command

case class StatsCommand(params: Map[String, String],
                        funcs: Seq[Expr],
                        by: Seq[Field] = Seq(),
                        dedupSplitVals: Boolean = false) extends Command

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

case class EventStatsCommand(params: Map[String, String], funcs: Seq[Expr], by: Seq[Field] = Seq()) extends Command

case class DedupCommand(numResults: Int,
                        fields: Seq[Field],
                        keepEvents: Boolean,
                        keepEmpty: Boolean,
                        consecutive: Boolean,
                        sortBy: SortCommand) extends Command

case class InputLookup(append: Boolean,
                       strict: Boolean,
                       start: Int,
                       max: Int,
                       tableName: String,
                       where: Option[Expr]) extends Command

case class Pipeline(commands: Seq[Command])