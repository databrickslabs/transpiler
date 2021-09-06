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

/**
 * @link https://docs.splunk.com/Documentation/SplunkCloud/8.2.2106/Search/Usetheevalcommandandfunctions
 * @link https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/CommonEvalFunctions
 */
case class EvalCommand(fields: Seq[(Value,Expr)]) extends Command
case class FieldConversion(func: String, field: Value, alias: Option[Value])
case class ConvertCommand(timeformat: Option[String], convs: Seq[FieldConversion]) extends Command
case class LookupOutput(kv: String, fields: Seq[Field])
case class LookupCommand(options: Map[String,String], dataset: String, fields: Seq[Field], output: Option[LookupOutput]) extends Command
case class CollectCommand(args: Map[String,String], fields: Seq[Value]) extends Command
case class WhereCommand(expr: Expr) extends Command
case class TableCommand(fields: Seq[Value]) extends Command
case class HeadCommand(evalExpr: Expr, keepLast: Bool = Bool(false), nullOption: Bool = Bool(false)) extends Command
case class FieldsCommand(op: Option[String], fields: Seq[Value]) extends Command
case class SortCommand(fieldsToSort: Seq[(Option[String], Expr)]) extends Command
/**
 * Documentation taken from:
 * @link https://docs.splunk.com/Documentation/SplunkCloud/8.2.2106/SearchReference/Stats
 * @param params
 *               [partitions=<num>] If specified, partitions the input data based on the split-by fields for
 *               multithreaded reduce. The partitions argument runs the reduce step (in parallel reduce processing)
 *               with multiple threads in the same search process on the same machine. Compare that with parallel
 *               reduce, using the redistribute command, that runs the reduce step in parallel on multiple machines.
 *               [allnum=<bool>] If true, computes numerical statistics on each field if and only if all of the values
 *               of that field are numerical.
 *               [delim=<string>] Specifies how the values in the list() or values() aggregation are delimited.
 * @param funcs  See Stats function options. The function can be applied to an eval expression,
 *               or to a field or set of fields. Use the AS clause to place the result into
 *               a new field with a name that you specify. You can use wild card characters
 *               in field names. For more information on eval expressions, see Types of eval
 *               expressions in the Search Manual.
 * @param by The name of one or more fields to group by. You cannot use a wildcard character to
 *           specify multiple fields with similar names. You must specify each field separately.
 *           The BY clause returns one row for each distinct value in the BY clause fields. If no
 *           BY clause is specified, the stats command returns only one row, which is the aggregation
 *           over the entire incoming result set.
 * @param dedupSplitVals Specifies whether to remove duplicate values in multivalued BY clause fields.
 */
case class StatsCommand(params: Map[String, String],
                        funcs: Seq[Expr],
                        by: Seq[Value] = Seq(),
                        dedupSplitVals: Boolean = false) extends Command

case class Pipeline(commands: Seq[Command])