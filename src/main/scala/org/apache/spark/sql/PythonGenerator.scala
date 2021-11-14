package org.apache.spark.sql

import scala.util.matching.Regex
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.UsingJoin
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

private case class GeneratorContext(maxLineWidth: Int = 120)

object PythonGenerator {
  private val pattern: Regex = "((?<![\\\\])['])".r

  def fromPlan(ctx: GeneratorContext, plan: LogicalPlan): String = plan match {
    case AppendData(table, query, writeOptions, isByName) =>
      s"${fromPlan(ctx, query)}\n.write.saveAsTable(${q(table.name)}, mode='append')"

    case p @ Project(exprs, child) =>
      val childCode = fromPlan(ctx, child)
      if (p.expressions.length - child.expressions.length == 1 || exprs.length == 1) {
        exprs.last match {
          case _: UnresolvedAttribute =>
            val columnNames = exprs.map(_.name).map(q).mkString(", ")
            s"$childCode\n.select($columnNames)"
          case Alias(child, name) =>
            child match {
              case UnresolvedAttribute(nameParts) =>
                if (nameParts.length > 1)
                  s"$childCode\n.withColumn(${q(name)}, ${expressionCode(child)})"
                else
                  s"$childCode\n.withColumnRenamed(${q(nameParts.mkString("."))}, ${q(name)})"
              case _ => s"$childCode\n.withColumn(${q(name)}, ${expressionCode(child)})"
            }
          case ur: UnresolvedRegex =>
            s"$childCode\n.selectExpr(${q(expression(ur))})"
          case _ =>
            throw new UnsupportedOperationException(s"cannot generate column: ${exprs.last}")
        }
      } else {
        s"$childCode\n.select(${exprCodeList(ctx, exprs)})"
      }

    case Filter(condition, child) =>
      fromPlan(ctx, child) + "\n" + unfoldWheres(condition)

    case Limit(expr, child) =>
      s"${fromPlan(ctx, child)}\n.limit($expr)"

    case Sort(order, global, child) =>
      val orderBy = order.map(item => {
        val dirStr = if (item.direction == Ascending) "asc()"  else "desc()"
        item.child match {
          case Cast(colExpr, dataType, _) =>
            s"F.col(${q(expression(colExpr))}).cast(${q(dataType.simpleString)}).$dirStr"
          case UnresolvedAttribute(nameParts) =>
            s"F.col(${q(nameParts.mkString("."))}).$dirStr"
        }
      })
      s"${fromPlan(ctx, child)}\n.orderBy(${orderBy.mkString(", ")})"

    case relation: UnresolvedRelation =>
      s"spark.table(${q(relation.name)})"

    case a: Aggregate =>
      // matching against class name, as not all Spark implementations have compatible ABI
      val grpExprsRev = a.groupingExpressions.map(_.toString)
      // Removing col used for grouping from the agg expression
      val aggExprRev = a.aggregateExpressions.filter(item => !grpExprsRev.contains(item.toString))
      val aggs = smartDelimiters(ctx, aggExprRev.map(expressionCode))
      val groupBy = exprList(ctx, a.groupingExpressions)
      s"${fromPlan(ctx, a.child)}\n.groupBy($groupBy)\n.agg($aggs)"

    case Join(left, right, joinType, _, _) =>
      // TODO: condition and hints are not yet supported
      val (tp, on) = joinType match {
        case UsingJoin(tp, usingColumns) => (tp, usingColumns)
        case tp => (tp, Seq())
      }
      val how = q(tp.sql.replace(" ", "_").toLowerCase)
      s"${fromPlan(ctx, left)}\n.join(${fromPlan(ctx, right)},\n${toPythonList(ctx, on)}, $how)"

    case FillNullShim(value, columns, child) =>
      val childCode = fromPlan(ctx, child)
      if (columns.isEmpty)
        s"$childCode\n.na.fill(${q(value)})"
      else
        s"$childCode\n.na.fill(${q(value)}, ${toPythonList(ctx, columns.toSeq)})"

    case UnknownPlanShim(message, child) =>
      s"${fromPlan(ctx, child)}\n# $message"
  }

  private def exprCodeList(ctx: GeneratorContext, exprs: Seq[Expression]) =
    smartDelimiters(ctx, exprs.map(expressionCode))

  private def exprList(ctx: GeneratorContext, exprs: Seq[Expression]) =
    smartDelimiters(ctx, exprs.map(expression).map(q))

  private def toPythonList(ctx: GeneratorContext, elements: Seq[String]): String =
    s"[${smartDelimiters(ctx, elements.map(q))}]"

  private def smartDelimiters(ctx: GeneratorContext, seq: Seq[String]) = {
    val default = seq.mkString(", ")
    if (default.length < ctx.maxLineWidth) default else seq.mkString(",\n  ")
  }

  private def unfoldWheres(expr: Expression): String = expr match {
    case And(left, right) => s"${unfoldWheres(left)}\n${unfoldWheres(right)}"
    // case _ => s".where(${q(expression(expr))})"
    case _ => s".where(${expressionCode(expr)})"
  }

  private def genSortOrderCode(sortOrder: SortOrder) = {
    sortOrder.direction match {
      case Ascending =>
        sortOrder.nullOrdering match {
          case NullsFirst =>
            s"${expressionCode(sortOrder.child)}.asc()"
          case NullsLast =>
            // default null ordering for `asc()` is NullsFirst
            s"${expressionCode(sortOrder.child)}.asc_null_last()"
        }
      case Descending =>
        sortOrder.nullOrdering match {
          case NullsFirst =>
            s"${expressionCode(sortOrder.child)}.desc_null_first()"
          case NullsLast =>
            // default null ordering for `desc()` is NullsLast
            s"${expressionCode(sortOrder.child)}.desc()"
        }
    }
  }

  private def genWindowSpecCode(ws: WindowSpecDefinition) = {
    val partGenCode = ws.partitionSpec.map(expressionCode).mkString(", ")
    val orderByGenCode = ws.orderSpec.map(expressionCode).mkString(", ")
    val windowGenCode = s"Window.partitionBy(${partGenCode}).orderBy(${orderByGenCode})"
    ws.frameSpecification match {
      case UnspecifiedFrame => windowGenCode
      case SpecifiedWindowFrame(frameType, lower, upper) =>
        frameType match {
          case RangeFrame =>
            s"${windowGenCode}.rangeBetween(${expressionCode(lower)}, ${expressionCode(upper)})"
          case RowFrame =>
            s"${windowGenCode}.rowsBetween(${expressionCode(lower)}, ${expressionCode(upper)})"
        }
    }
  }

  private val jvmToPythonOverrides = Map(
    "&&" -> "&",
    "=" -> "==",
    "||" -> "|"
  )

  private def expressionCode(expr: Expression): String = expr match {
    case b: BinaryOperator =>
      val symbol = jvmToPythonOverrides.getOrElse(b.symbol, b.symbol)
      s"(${expressionCode(b.left)} $symbol ${expressionCode(b.right)})"
    case Size(left, _) =>
      s"F.size(${expressionCode(left)})"
    case ArrayFilter(left, LambdaFunction(fn, args, _)) =>
      // Look for _invoke_higher_order_function() in pyspark/sql/functions.py
      s"F.filter(${expressionCode(left)}, lambda ${args.map(expression).mkString(",")}: ${expressionCode(fn)})"
    case In(attr, items) =>
      s"${expressionCode(attr)}.isin(${items.map(expressionCode).mkString(", ")})"
    case Alias(child, name) =>
      s"${expressionCode(child)}.alias('$name')"
    case UnresolvedAlias(child, aliasFunc) =>
      expressionCode(child)
    case RLike(left, right) =>
      s"${expressionCode(left)}.rlike(${expressionCode(right)})"
    case Literal(value, t @ BooleanType) =>
      val pyBool = if (value.asInstanceOf[Boolean]) "True" else "False"
      s"F.lit($pyBool)"
    case Literal(value, t @ IntegerType) =>
      s"F.lit($value)"
    case Literal(value, t @ StringType) =>
      s"F.lit(${q(value.toString)})"
    case Alias(child, name) =>
      s"${expressionCode(child)}.alias(${q(name)})"
    case Count(children) =>
      s"F.count(${children.map(expressionCode).mkString(", ")})"
    case Sum(child) =>
      s"F.sum(${expressionCode(child)})"
    case Length(child) =>
      s"F.length(${expressionCode(child)})"
    case Size(child, boolean) =>
      s"F.size(${expressionCode(child)})"
    case Cast(colExpr, dataType, _) =>
      s"F.col(${q(expression(colExpr))}).cast(${q(dataType.simpleString)})"
    case Min(expr) =>
      s"F.min(${expressionCode(expr)})"
    case Max(expr) =>
      s"F.max(${expressionCode(expr)})"
    case MonotonicallyIncreasingID() =>
      s"F.monotonically_increasing_id()"
    case Concat(children) =>
      s"F.concat(${children.map(expressionCode).mkString(", ")})"
    case ArrayJoin(array, delimiter, nullReplacement) =>
      s"F.array_join(${expressionCode(array)}, ${delimiter.sql})"
    case CollectList(child, x, y) =>
      s"F.collect_list(${expressionCode(child)})"
    case RowNumber() =>
      s"F.row_number()"
    case AggregateExpression(aggFn, mode, isDistinct, filter, resultId) =>
      expressionCode(aggFn)
    case CurrentRow =>
      "Window.currentRow"
    case UnboundedFollowing =>
      "Window.unboundedFollowing"
    case UnboundedPreceding =>
      "Window.unboundedPreceding"
    case so: SortOrder =>
      genSortOrderCode(so)
    case ur: UnresolvedRegex =>
      s"F.col(${q(s"`${ur.regexPattern}`")})"
    case RegExpExtract(subject, regexp, idx) =>
      s"F.regexp_extract(${expressionCode(subject)}, ${regexp.sql}, ${idx.sql})"
    case WindowExpression(windowFunction, windowSpec) =>
      s"${expressionCode(windowFunction)}.over(${expressionCode(windowSpec)})"
    case ws: WindowSpecDefinition =>
      genWindowSpecCode(ws)
    case namedStruct: CreateNamedStruct =>
      s"F.struct(${namedStruct.valExprs.map(expressionCode).mkString(", ")})"
    case fs: FormatString =>
      val items = fs.children.toList
      s"F.format_string(${q(items.head.toString())}, ${items.tail.map(expressionCode).mkString(", ")})"
    case attr: AttributeReference =>
      s"F.col(${q(attr.name)})"
    case attr: UnresolvedAttribute =>
      s"F.col(${q(attr.name)})"
    case TimeWindow(col, window, slide, _) if window == slide =>
      val interval = IntervalUtils.stringToInterval(UTF8String.fromString(s"$window microseconds"))
      s"F.window(${expressionCode(col)}, '$interval')"
    case UnresolvedNamedLambdaVariable(nameParts) =>
      nameParts.mkString(", ")
    case _ => s"F.expr(${q(expr.sql)})"
  }

  /** Simplified SQL rendering of Spark expressions */
  def expression(expr: Expression): String = expr match {
    case EqualTo(attr: UnresolvedAttribute, value) =>
      s"${attr.name} = ${expression(value)}"
    case In(attr: UnresolvedAttribute, items) =>
      s"${attr.name} IN (${items.map(expression).mkString(", ")})"
    case UnresolvedAlias(child, aliasFunc) => expression(child)
    case Alias(child, name) => s"${expression(child)} AS $name"
    case RLike(left, right) => s"${left.sql} RLIKE ${right.sql}"
    case UnresolvedRegex(regexPattern, table, caseSensitive) => s"`$regexPattern`"
    case attr: AttributeReference => attr.name
    case a: UnresolvedAttribute => a.name
    case _: Any => expr.sql
  }

  /** Sugar for quoting strings */
  private def q(value: String) =
    if (pattern.findAllIn(value).toList.isEmpty)
      "'" + value + "'" else "\"" + value + "\""
}
