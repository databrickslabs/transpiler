package spl

import org.apache.spark.sql.catalyst.trees.TreeNode

abstract class SplNode extends TreeNode[SplNode] {
  override def verboseString(maxFields: Int): String = toString
  override def simpleStringWithNodeId(): String =
    throw new UnsupportedOperationException(s"SplNode does not implement simpleStringWithNodeId")
}

abstract sealed class Expr extends SplNode
abstract sealed class LeafExpr extends Expr {
  override def children: Seq[SplNode] = Nil
}

sealed trait Field
abstract sealed class Constant extends LeafExpr

case class Null() extends Constant
case class Bool(value: Boolean) extends Constant
case class IntValue(value: Int) extends Constant
case class Value(value: String) extends Constant with Field
case class FV(field: String, value: String) extends LeafExpr

case class AliasedField(field: Value, alias: String) extends Expr with Field {
  override def children: Seq[SplNode] = Seq(field)
}

case class FvList(fvs: Seq[FV]) extends Expr {
  override def children: Seq[SplNode] = fvs
}

case class Binary(left: Expr, symbol: OperatorSymbol, right: Expr) extends Expr {
  override def children: Seq[SplNode] = Seq(left, right)
}
case class Unary(symbol: OperatorSymbol, right: Expr) extends Expr {
  override def children: Seq[SplNode] = Seq(right)
}
case class Call(name: String, args: Seq[Expr] = Seq()) extends Expr {
  override def children: Seq[SplNode] = args
}

case class FieldIn(field: String, exprs: Seq[Expr]) extends Expr {
  override def children: Seq[SplNode] = exprs
}

case class Alias(expr: Expr, name: String) extends Expr with Field {
  override def children: Seq[SplNode] = Seq(expr)
}

abstract sealed class Command extends SplNode

case class SearchCommand(expr: Expr) extends Command {
  override def children: Seq[SplNode] = Seq(expr)
}

/**
 * @link https://docs.splunk.com/Documentation/SplunkCloud/8.2.2106/Search/Usetheevalcommandandfunctions
 * @link https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/CommonEvalFunctions
 */
case class EvalCommand(fields: Seq[(Value,Expr)]) extends Command {
  override def children: Seq[SplNode] = fields.map(_._2)
}

case class FieldConversion(func: String, field: Value, alias: Option[Value]) extends SplNode {
  override def children: Seq[SplNode] = Seq(field)
}

case class ConvertCommand(timeformat: Option[String], convs: Seq[FieldConversion]) extends Command {
  override def children: Seq[SplNode] = convs
}

case class LookupOutput(kv: String, fields: Seq[Field])

case class LookupCommand(dataset: String, fields: Seq[Field], output: Option[LookupOutput]) extends Command {
  /** Does this lookup command have any aliases from the right side of the join? */
  def hasOutput: Boolean = output.isDefined
  override def children: Seq[SplNode] = fields.map {
    case v: Value => v
    case v: AliasedField => v
    case v: Alias => v
  }
}

case class CollectCommand(args: Map[String,String], fields: Seq[Value]) extends Command {
  override def children: Seq[SplNode] = fields
}

case class WhereCommand(expr: Expr) extends Command {
  override def children: Seq[SplNode] = Seq(expr)
}

case class TableCommand(fields: Seq[Value]) extends Command {
  override def children: Seq[SplNode] = fields
}

case class HeadCommand(evalExpr: Expr,
                       keepLast: Bool = Bool(false),
                       nullOption: Bool = Bool(false)) extends Command {
  override def children: Seq[SplNode] = Seq(evalExpr)
}

case class FieldsCommand(op: Option[String], fields: Seq[Value]) extends Command {
  override def children: Seq[SplNode] = fields
}

case class SortCommand(fieldsToSort: Seq[(Option[String], Expr)]) extends Command {
  override def children: Seq[SplNode] = fieldsToSort.map(_._2)
}

case class StatsCommand(params: Map[String, String],
                        funcs: Seq[Expr],
                        by: Seq[Value] = Seq(),
                        dedupSplitVals: Boolean = false) extends Command {
  override def children: Seq[SplNode] = funcs
}

case class RexCommand(field: Option[String],
                      maxMatch: Int,
                      offsetField: Option[String],
                      mode: Option[String],
                      regex: String) extends Command {
  override def children: Seq[SplNode] = Nil
}

case class RenameCommand(alias: Seq[Alias]) extends Command {
  override def children: Seq[SplNode] = alias
}

case class RegexCommand(item: Option[(Value, String)], regex: String) extends Command {
  override def children: Seq[SplNode] = Nil
}

case class JoinCommand(joinType: String = "inner",
                       useTime: Boolean = false,
                       earlier: Boolean = true,
                       overwrite: Boolean = true,
                       max: Int = 1,
                       fields: Seq[Value],
                       subSearch: Pipeline) extends Command {
  override def children: Seq[SplNode] = Seq(subSearch)
}

case class ReturnCommand(count: Option[IntValue], fields: Seq[Product with Serializable]) extends Command {
  // TODO: replace "Product with Serializable" with "Field" and refactor things
  override def children: Seq[SplNode] = Nil
}

// TODO: Option[Seq[Value]] -> Seq[Value] = Seq()
case class FillNullCommand(value: Option[String], fields: Option[Seq[Value]]) extends Command {
  override def children: Seq[SplNode] = fields.getOrElse(Seq())
}

case class Pipeline(commands: Seq[Command]) extends SplNode {
  override def children: Seq[SplNode] = commands
}
