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
           splFieldToAttr: Field => NamedExpression = this.splFieldToAttr,
           analyzePlan: LogicalPlan => Seq[Attribute] = this.analyzePlan,
           output: Seq[NamedExpression] = this.output): LogicalContext =

    new LogicalContext(indexName, timeFieldName, rawFieldName, searchVariables,
      wcStrToRex, splFieldToAttr, analyzePlan, output)

  def containsWildcard(expr: Expr): Boolean = expr match {
    case Wildcard(_) => true
    case Field(name) => name.contains("*")
    case FieldConversion(_, Field(name), _) => name.contains("*")
    case Call(_, args) => args.map(containsWildcard).contains(true)
    case Alias(expr, name) => containsWildcard(expr) || name.contains("*")
    case _ => false
  }

  def expandWildcards[E <: Expr](expr: E): Seq[Expr] = expr match {
    case Wildcard(value) =>
      val regex = this.wcStrToRex(value).toString()
      this.output.filter(_.name matches regex).map(f => Field(f.name))
    case Field(value) => expandWildcards(Wildcard(value))
    case Call(name, _ @ Seq(wc: Wildcard)) => expandWildcards(wc).map(f => Call(name, Seq(f)))
    case Alias(call @ Call(_, _ @ Seq(wc: Wildcard)), wcName) =>
      val expandedCalls = expandWildcards(call).asInstanceOf[Seq[Call]]
      expandedCalls.map(f => Alias(f,
        expandAliasWc(wcName, wc.value, f.args.head.asInstanceOf[Field].value)))
    case FieldConversion(func, field, alias) =>
      expandWildcards(field).map(f => FieldConversion(func, f.asInstanceOf[Field], alias))
    case _ => Seq(expr)
  }

  private def expandAliasWc(aliasName: String, fctWc: String, fctExp: String): String = {
    if (fctWc.count(_ == '*') != aliasName.count(_ == '*')) {
      throw new ConversionFailure("Resolved wildcards between field specifier" +
        " and rename specifier do not match")
    }
    val groups = this.wcStrToRex(fctWc).findAllMatchIn(fctExp)
    aliasName flatMap {case '*' => s"${groups.next().group(1)}" case c => s"$c"}
  }
}
