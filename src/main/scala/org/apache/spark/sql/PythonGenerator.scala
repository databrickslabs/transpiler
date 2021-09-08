package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRegex, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, LeftAnti, LeftOuter, LeftSemi, NaturalJoin, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

class PythonGenerator {
  var prevExprs = Set[ExprId]()

  def fromPlan(plan: LogicalPlan): String = plan match {
    case AppendData(table, query, writeOptions, isByName) =>
      // TODO enclose in parentheses one level above this
      s"${fromPlan(query)}\n.write.saveAsTable(${q(table.name)}, mode='append')"

    case Project(exprs, child) =>
      val childCode = fromPlan(child)
      val currExprs = exprs.filter(!_.isInstanceOf[Unevaluable]).map(_.exprId).toSet
      val newExprs = currExprs.diff(prevExprs)
      prevExprs = currExprs
      if (newExprs.size == 1) {
        val withColumn = exprs.filter(_.exprId == newExprs.head).head
        s"$childCode\n.withColumn(${q(withColumn.name)}, ${expressionCode(withColumn.children.head)})"
      } else {
        s"$childCode\n.selectExpr(${exprList(exprs)})"
      }

    case Filter(condition, child) =>
      fromPlan(child) + "\n" + unfoldWheres(condition)

    case Limit(expr, child) =>
      s"${fromPlan(child)}\n.limit($expr)"

    case Sort(order, global, child) =>
      val orderBy = order.map(item => {
        val dirStr = if (item.direction == Ascending) "asc()"  else "desc()"
        item.child match {
          case Cast(colExpr, dataType, _) =>
            s"F.col(${q(colExpr.toString())}).cast(${q(dataType.simpleString)}).${dirStr}"
          case UnresolvedAttribute(nameParts) =>
            s"F.col(${q(nameParts.mkString("."))}).${dirStr}"
        }
      })
      s"${fromPlan(child)}\n.orderBy(${orderBy.mkString(", ")})"

    case relation: UnresolvedRelation =>
      s"spark.table(${q(relation.name)})"

    case Aggregate(by, agg, child) =>
      val aggs = agg.map(expressionCode).mkString(", ")
      s"${fromPlan(child)}\n.groupBy(${exprList(by)})\n.agg($aggs)"

    case Join(left, right, joinType, _, _) =>
      // TODO: condition and hints are not yet supported
      val (tp, on) = joinType match {
        case UsingJoin(tp, usingColumns) => (tp, usingColumns)
        case tp => (tp, Seq())
      }
      val how = q(tp.sql.replace(" ", "_").toLowerCase)
      s"${fromPlan(left)}\n.join(${fromPlan(right)},\n[${on.map(q).mkString(", ")}], $how)"
  }

  private def exprList(exprs: Seq[Expression]) =
    exprs.map(expression).map(q).mkString(", ")

  private def expressionCode(expr: Expression): String = expr match {
    case Literal(value, t @ BooleanType) =>
      val pyBool = if (value.asInstanceOf[Boolean]) "True" else "False"
      s"F.lit($pyBool)"
    case Literal(value, t @ IntegerType) =>
      s"F.lit($value)"
    case Literal(value, t @ StringType) =>
      s"F.lit(${q(value.toString)})"
    case _ => s"F.expr(${q(expr.sql)})"
  }

  private def unfoldWheres(expr: Expression): String = expr match {
    case And(left, right) => s"${unfoldWheres(left)}\n.where(${q(expression(right))})"
    case _ => s".where(${q(expression(expr))})"
  }

//  org.apache.spark.sql.catalyst.util.toPrettySQL(expr)

  def expression(expr: Expression): String = expr match {
    case UnresolvedAlias(child, aliasFunc) => expression(child)
    case Alias(child, name) => s"${expression(child)} AS $name"
    case UnresolvedRegex(regexPattern, table, caseSensitive) => s"`${regexPattern}`"
    case a: UnresolvedAttribute => a.name
    case _: Any => expr.sql
  }

  /**
   * Sugar for quoting strings
   * TODO: make it smarter and use double quotes when needed
   */
  private def q(value: String) = "'" + value + "'"
}
