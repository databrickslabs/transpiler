package com.databricks.labs.transpiler.spl.catalyst

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import com.databricks.labs.transpiler.spl.ast.Command

class ConversionFailure(msg: String) extends Exception(msg)

case class EmptyContextOutput(command: Command) extends Exception {
  override def getMessage: String =
    s"Unable to tanslate ${command.getClass.getCanonicalName} due to empty context output"
}

/** Catch-all error node to continue transpiling as much as possible */
case class UnknownPlanShim(t: String, child: LogicalPlan) extends LogicalPlan {
  override def output: Seq[Attribute] = child.output
  override def children: Seq[LogicalPlan] = child.children

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[LogicalPlan]
  ): LogicalPlan = copy(t, newChildren(0))
}
