package spl.catalyst

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import spl.ast.Command

class ConversionFailure(msg: String) extends Exception(msg)

case class EmptyContextOutput(command: Command) extends Exception {
  override def getMessage: String =
    s"Unable to tanslate ${command.getClass.getCanonicalName} due to empty context output"
}

/**
 * Partial generation is better than none: Catch-all error
 * to help continuing the process. Results in comment for
 * generated code or AnalysisException for executed plan.
 */
case class UnknownPlanShim(t: String, child: LogicalPlan) extends LogicalPlan {
  override def output: Seq[Attribute] = child.output
  override def children: Seq[LogicalPlan] = child.children
}