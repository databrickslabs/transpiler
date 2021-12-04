package spl.catalyst

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import spl.ast.{Alias, Call, Expr, Field, FieldConversion, Wildcard}

import scala.util.matching.Regex

private[spl] case class VariableAlias(name: String, alias: String)

private[spl] class LogicalContext(
     val indexName: String = "main",
     val timeFieldName: String = "_time",
     val rawFieldName: String = "_raw",
     var searchVariables: Seq[VariableAlias] = Seq(),
     val wcStrToRex: String => Regex = s => s.replace("*", "(\\w*)").r,
     val splFieldToAttr: Field => NamedExpression = field => UnresolvedAttribute(Seq(field.value)),
     val analyzePlan: LogicalPlan => Seq[Attribute] = (_: LogicalPlan) => Seq[Attribute](),
     var output: Seq[NamedExpression] = Seq()) {
  def copy(indexName: String = this.indexName,
           timeFieldName: String = this.timeFieldName,
           rawFieldName: String = this.rawFieldName,
           searchVariables: Seq[VariableAlias] = this.searchVariables,
           wcStrToRex: String => Regex = this.wcStrToRex,
           splFieldToAttr: (Field) => NamedExpression = this.splFieldToAttr,
           analyzePlan: (LogicalPlan) => Seq[Attribute] = this.analyzePlan,
           output: Seq[NamedExpression] = this.output): LogicalContext =

    new LogicalContext(indexName, timeFieldName, rawFieldName, searchVariables,
      wcStrToRex, splFieldToAttr, analyzePlan, output)

  def containsWildcard(expr: Expr): Boolean = expr match {
    case Wildcard(_) => true
    case Field(name) => name.contains("*")
    case FieldConversion(_, field, _) => field.value.contains("*")
    case Call(_, args) => args.map(containsWildcard).contains(true)
    case Alias(expr, name) => containsWildcard(expr) || name.contains("*")
    case _ => false
  }

  def expandWildcards[E <: Expr](expr: E): Seq[Expr] = expr match {
    case Wildcard(value) =>
      val regex = wcStrToRex(value).toString()
      this.output.filter(_.name matches regex).map(f => Field(f.name))
    case Field(value) => expandWildcards(Wildcard(value))
    case Call(name, args) => args.head match {
      case wc: Wildcard => expandWildcards(wc).map(f => Call(name, Seq(f)))
      case field: Field => expandWildcards(field).map(f => Call(name, Seq(f)))
      case _ => Seq(expr)
    }
    case Alias(call: Call, wcName) =>
      val fctWcName = call.args.head.asInstanceOf[Wildcard].value
      val expandedCalls = expandWildcards(call).asInstanceOf[Seq[Call]]
      expandedCalls.map(f => Alias(f,
        expandAliasWc(wcName, fctWcName, f.args.head.asInstanceOf[Field].value)))
    case FieldConversion(func, field, alias) =>
      expandWildcards(field).map(f => FieldConversion(func, f.asInstanceOf[Field], alias))
    case _ => Seq(expr)
  }

  private def expandAliasWc(aliasName: String, fctWc: String, fctExp: String): String = {
    val regex: Regex = this.wcStrToRex(fctWc)
    if (regex.findAllMatchIn(fctExp).size != aliasName.count(_ == '*')) {
      throw new ConversionFailure("Resolved wildcards between field specifier" +
        " and rename specifier do not match")
    }
    val groups = regex.findAllMatchIn(fctExp)
    aliasName flatMap {case '*' => s"${groups.next().group(1)}" case c => s"$c"}
  }
}
