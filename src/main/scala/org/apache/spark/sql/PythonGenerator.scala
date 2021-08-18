package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

class PythonGenerator {
  var prevExprs = Set[ExprId]()

  def fromPlan(plan: LogicalPlan): String = plan match {
    case AppendData(table, query, writeOptions, isByName) =>
      s"(${fromPlan(query)}\n.write.saveAsTable(${quoted(table.name)}, mode='append'))"
    case Project(exprs, child) =>
      val childCode = fromPlan(child)
      val currExprs = exprs.filter(!_.isInstanceOf[Unevaluable]).map(_.exprId).toSet
      val newExprs = currExprs.diff(prevExprs)
      prevExprs = currExprs
      if (newExprs.size == 1) {
        val withColumn = exprs.filter(_.exprId == newExprs.head).head
        s"$childCode\n.withColumn(${quoted(withColumn.name)}, ${expressionCode(withColumn.children.head)})"
      } else {
        s"$childCode\n.selectExpr(${exprs.map(expression).map(quoted).mkString(", ")})"
      }
    case Filter(condition, child) =>
      fromPlan(child) + "\n" + unfoldWheres(condition)
    case relation: UnresolvedRelation =>
      s"spark.table(${quoted(relation.name)})"
  }

  private def expressionCode(expr: Expression): String = expr match {
    case Literal(value, t @ BooleanType) =>
      val pyBool = if (value.isInstanceOf[Boolean]) "True" else "False"
      s"F.lit($pyBool)"
    case Literal(value, t @ IntegerType) =>
      s"F.lit($value)"
    case Literal(value, t @ StringType) =>
      s"F.lit(${quoted(value.toString)})"
    case _ => s"F.expr(${quoted(expr.sql)})"
  }

  private def unfoldWheres(expr: Expression): String = expr match {
    case And(left, right) => s"${unfoldWheres(left)}\n.where(${quoted(expression(right))})"
    case _ => s".where(${quoted(expression(expr))})"
  }

//  org.apache.spark.sql.catalyst.util.toPrettySQL(expr)

  def expression(expr: Expression): String = expr match {
    case UnresolvedAlias(child, aliasFunc) => expression(child)
    case Alias(child, name) => s"${expression(child)} AS $name"
    case a: UnresolvedAttribute => a.name
    case _: Any => expr.sql
  }

  private def quoted(value: String) = '"' + value + '"'
}
