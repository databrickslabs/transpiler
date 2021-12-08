package spl.pyspark

import org.apache.spark.sql.{CidrMatch, FillNullShim}

import scala.util.matching.Regex
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.UsingJoin
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, NullType, StringType}
import spl.catalyst.UnknownPlanShim

case class GeneratorContext(maxLineWidth: Int = 120)

object PythonGenerator {
  private val pattern: Regex = "((?<![\\\\])['])".r

  def fromPlan(ctx: GeneratorContext, plan: LogicalPlan): String = plan match {
    case AppendData(table, query, writeOptions, isByName) =>
      s"${fromPlan(ctx, query)}\n.write.saveAsTable(${q(table.name)}, mode='append')"

    case SubqueryAlias(identifier, child) =>
      s"${fromPlan(ctx, child)}.alias(${q(identifier.name)})"

    case p @ Project(exprs, child) =>
      val childCode = fromPlan(ctx, child)
      if (p.expressions.length - child.expressions.length == 1 || exprs.length == 1) {
        exprs.last match {
          case _: UnresolvedAttribute =>
            val columnNames = exprs.map(_.name).map(q).mkString(", ")
            s"$childCode\n.select($columnNames)"
          case Alias(UnresolvedAttribute(nameParts), name) if nameParts.length == 1 =>
            s"$childCode\n.withColumnRenamed(${q(nameParts.mkString("."))}, ${q(name)})"
          case Alias(child, name) =>
            child match {
              case UnresolvedAttribute(nameParts) =>
                if (nameParts.length > 1) {
                  s"$childCode\n.withColumn(${q(name)}, ${expressionCode(child)})"
                } else {
                  s"$childCode\n.withColumnRenamed(${q(nameParts.mkString("."))}, ${q(name)})"
                }
              case _ => s"$childCode\n.withColumn(${q(name)}, ${expressionCode(child)})"
            }
          case ur: UnresolvedRegex =>
            s"$childCode\n.selectExpr(${q(expression(ur))})"
          case us: UnresolvedStar =>
            us.target match {
              case Some(values) =>
                val starList = values.map(id => {
                  val starExpr = s"${id}.*"
                  s"F.col(${q(starExpr)})"
                }).mkString(", ")
                s"$childCode\n.select(${starList})"
              case None => ""
            }
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
        val dirStr = if (item.direction == Ascending) {
          "asc()"
        } else "desc()"
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

    case Join(left, right, joinType, condition, _) =>
      // TODO: condition and hints are not yet supported
      val (tp, on) = joinType match {
        case UsingJoin(tp, usingColumns) => (tp, usingColumns)
        case tp => (tp, Seq())
      }
      val how = q(tp.sql.replace(" ", "_").toLowerCase)
      condition match {
        case Some(exp) =>
          s"""${fromPlan(ctx, left)}
             |.join(${fromPlan(ctx, right)},
             |${unfoldJoinCondition(exp)},
             |$how)""".stripMargin
        case None =>
          s"${fromPlan(ctx, left)}\n.join(${fromPlan(ctx, right)},\n${toPythonList(ctx, on)}, $how)"
      }

    case r: Range =>
      s"spark.range(${r.start}, ${r.end}, ${r.step})"

    case Union(children, byName, allowMissingCol) =>
      if (byName) {
        val unionArg = if (allowMissingCol) ", allowMissingColumns=True" else ""
        children.map(fromPlan(ctx, _)).reduce((l, r) => s"$l.unionByName($r$unionArg)")
      }
      else children.map(fromPlan(ctx, _)).reduce((l, r) => s"$l.union($r)")

    case FillNullShim(value, columns, child) =>
      val childCode = fromPlan(ctx, child)
      if (columns.isEmpty) {
        s"$childCode\n.na.fill(${q(value)})"
      } else {
        s"$childCode\n.na.fill(${q(value)}, ${toPythonList(ctx, columns.toSeq)})"
      }

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

  private def unfoldJoinCondition(expr: Expression): String = expr match {
    case And(left, right) => s"${unfoldJoinCondition(left)} && ${unfoldJoinCondition(right)}"
    case _ => s"${expressionCode(expr)}"
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
    val windowGenCode = s"Window.partitionBy($partGenCode).orderBy($orderByGenCode)"
    ws.frameSpecification match {
      case UnspecifiedFrame => windowGenCode
      case SpecifiedWindowFrame(frameType, lower, upper) =>
        frameType match {
          case RangeFrame =>
            s"$windowGenCode.rangeBetween(${expression(lower)}, ${expression(upper)})"
          case RowFrame =>
            s"$windowGenCode.rowsBetween(${expression(lower)}, ${expression(upper)})"
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
    case Last(child, ignoreNulls) =>
      val pyBool = if (ignoreNulls.asInstanceOf[Boolean]) "True" else "False"
      s"F.last(${expressionCode(child)}, $pyBool)"
    case First(child, ignoreNulls) =>
      val pyBool = if (ignoreNulls.asInstanceOf[Boolean]) "True" else "False"
      s"F.first(${expressionCode(child)}, $pyBool)"
    case ArrayFilter(left, LambdaFunction(fn, args, _)) =>
      s"F.filter(${expressionCode(left)}, " +
        s"lambda ${args.map(expression).mkString(",")}: ${expressionCode(fn)})"
    case CaseWhen(branches: Seq[(Expression, Expression)], elseValue: Option[Expression]) =>
      caseWhenRep(branches, elseValue)
    case In(attr, items) =>
      s"${expressionCode(attr)}.isin(${items.map(expressionCode).mkString(", ")})"
    case UnresolvedAlias(child, aliasFunc) =>
      expressionCode(child)
    case RLike(left, right) =>
      s"${expressionCode(left)}.rlike(${expressionCode(right)})"
    case Literal(value, _ @ BooleanType) =>
      val pyBool = if (value.asInstanceOf[Boolean]) "True" else "False"
      s"F.lit($pyBool)"
    case Literal(value, _ @ IntegerType) =>
      s"F.lit($value)"
    case Literal(value, _ @ DoubleType) =>
      s"F.lit($value)"
    case Literal(value, _ @ StringType) =>
      s"F.lit(${q(value.toString)})"
    case Literal(_, _ @ NullType) =>
      s"F.lit(None)"
    case Alias(child, name) =>
      s"${expressionCode(child)}.alias(${q(name)})"
    case Count(children) =>
      s"F.count(${children.map(expressionCode).mkString(", ")})"
    case Round(child, scale) =>
      s"F.round(${expressionCode(child)}, ${expression(scale)})"
    case Sum(child) =>
      s"F.sum(${expressionCode(child)})"
    case Length(child) =>
      s"F.length(${expressionCode(child)})"
    case Size(child, _) =>
      s"F.size(${expressionCode(child)})"
    case Cast(colExpr, dataType, _) =>
      s"${expressionCode(colExpr)}.cast(${q(dataType.simpleString)})"
    case Min(expr) =>
      s"F.min(${expressionCode(expr)})"
    case Max(expr) =>
      s"F.max(${expressionCode(expr)})"
    case Least(children) =>
      s"F.least(${children.map(expressionCode).mkString(", ")})"
    case Greatest(children) =>
      s"F.greatest(${children.map(expressionCode).mkString(", ")})"
    case MonotonicallyIncreasingID() =>
      s"F.monotonically_increasing_id()"
    case Concat(children) =>
      s"F.concat(${children.map(expressionCode).mkString(", ")})"
    case ArrayJoin(array, delimiter, nullReplacement) =>
      s"F.array_join(${expressionCode(array)}, ${delimiter.sql})"
    case CollectList(child, x, y) =>
      s"F.collect_list(${expressionCode(child)})"
    case CollectSet(child, _, _) =>
      s"F.collect_set(${expressionCode(child)})"
    case RowNumber() =>
      s"F.row_number()"
    case DateFormatClass(left, right, _) =>
      s"F.date_format(${expressionCode(left)}, ${expression(right)})"
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
    case RegExpReplace(subject, regexp, rep, _) =>
      s"F.regexp_replace(${expressionCode(subject)}, ${regexp.sql}, ${rep.sql})"
    case WindowExpression(windowFunction, windowSpec) =>
      s"${expressionCode(windowFunction)}.over(${expressionCode(windowSpec)})"
    case ws: WindowSpecDefinition =>
      genWindowSpecCode(ws)
    case namedStruct: CreateNamedStruct =>
      s"F.struct(${namedStruct.valExprs.map(expressionCode).mkString(", ")})"
    case fs: FormatString =>
      val items = fs.children.toList
      val exprs = items.tail.map(expressionCode).mkString(", ")
      s"F.format_string(${q(items.head.toString())}, $exprs)"
    case attr: AttributeReference =>
      s"F.col(${q(attr.name)})"
    case attr: UnresolvedAttribute =>
      s"F.col(${q(attr.name)})"
    case attr: UnresolvedNamedLambdaVariable =>
      s"${attr.name}"
    case TimeWindow(col, window, slide, _) if window == slide =>
      val interval = IntervalUtils.stringToInterval(
        UTF8String.fromString(s"$window microseconds"))
      s"F.window(${expressionCode(col)}, '$interval')"
    case Explode(child) =>
      s"F.explode(${expressionCode(child)})"
    case Substring(str, pos, len) =>
      s"F.substring(${expressionCode(str)}, ${expression(pos)}, ${expression(len)})"
    case IsNotNull(child) =>
      s"${expressionCode(child)}.isNotNull()"
    case IsNull(child) =>
      s"${expressionCode(child)}.isNull()"
    case CurrentTimestamp() =>
      s"F.current_timestamp()"
    case CidrMatch(cidr, ip) =>
      "F.expr(" + "\"" + s"cidr_match(${expression(cidr)}, ${expression(ip)})" + "\")"
    case Upper(child) =>
      s"F.upper(${expressionCode(child)})"
    case _ =>
      s"F.expr(${q(expr.sql)})"
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
    case CurrentRow => "Window.currentRow"
    case UnboundedFollowing => "Window.unboundedFollowing"
    case UnboundedPreceding => "Window.unboundedPreceding"
    case attr: AttributeReference => attr.name
    case a: UnresolvedAttribute => a.name
    case _: Any => expr.sql
  }

  private def caseWhenRep(branches: Seq[(Expression, Expression)],
                          elseValue: Option[Expression]): String = {
    val otherwiseStmt = if (elseValue.isDefined) {
      s".otherwise(${expressionCode(elseValue.get)})"
    } else ""
    val nl = if (branches.size > 1) "\n" else ""
    val whenStmts = "F" + branches
      .map(t => s".when(${expressionCode(t._1)}, ${expressionCode(t._2)})" + nl)
      .mkString("")
    whenStmts + otherwiseStmt
  }

  /** Sugar for quoting strings */
  private def q(value: String) =
    if (pattern.findAllIn(value).toList.isEmpty) {
      "'" + value + "'"
    } else "\"" + value + "\""
}
