package spl

sealed trait Expr
sealed trait Constant extends Expr
case class Null() extends Constant
case class Bool(value: Boolean) extends Constant

sealed trait Field
case class Value(value: String) extends Constant with Field
case class AliasedField(field: Value, alias: String) extends Expr with Field
case class FV(field: String, value: String) extends Expr
case class FvList(fvs: Seq[FV]) extends Expr
case class IntValue(value: Int) extends Constant

case class Binary(left: Expr, symbol: OperatorSymbol, right: Expr) extends Expr
case class Unary(symbol: OperatorSymbol, right: Expr) extends Expr
case class Call(name: String, args: Seq[Expr] = Seq()) extends Expr
case class FieldIn(field: String, exprs: Seq[Expr]) extends Expr
case class Alias(expr: Expr, name: String) extends Expr with Field

sealed trait Command
case class SearchCommand(expr: Expr) extends Command
case class EvalCommand(fields: Seq[(Value,Expr)]) extends Command
case class FieldConversion(func: String, field: Value, alias: Option[Value])
case class ConvertCommand(timeformat: Option[String], convs: Seq[FieldConversion]) extends Command
case class LookupOutput(kv: String, fields: Seq[Field])
case class LookupCommand(options: Map[String,String], dataset: String, fields: Seq[Field], output: Option[LookupOutput]) extends Command
case class CollectCommand(args: Map[String,String], fields: Seq[Value]) extends Command
case class WhereCommand(expr: Expr) extends Command
case class TableCommand(fields: Seq[Value]) extends Command
case class StatsCommand(params: Map[String, String], funcs: Seq[Expr], by: Seq[Value] = Seq(), dedupSplitVals: Boolean = false) extends Command

case class Pipeline(commands: Seq[Command])