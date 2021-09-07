package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
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
        s"$childCode\n.selectExpr(${exprs.map(expression).map(q).mkString(", ")})"
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
  }

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
    case a: UnresolvedAttribute => a.name
    case _: Any => expr.sql
  }

  /**
   * Sugar for quoting strings
   * TODO: make it smarter and use double quotes when needed
   */
  private def q(value: String) = "'" + value + "'"
}
