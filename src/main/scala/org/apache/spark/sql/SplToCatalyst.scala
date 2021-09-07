package org.apache.spark.sql

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRegex, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Cast, Descending, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Filter, Limit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.{expressions => e}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

class SplToCatalyst extends Logging {
  /**
   * Currently projected expressions
   * TODO: https://github.com/databricks/spark-spl/issues/2
   */
  private var output = Seq[e.NamedExpression]()

  def selectExpr(fields: Seq[spl.Value], tree: LogicalPlan) = {
    Project(fields.map {
      case spl.Value(value) => Column(value)
    }.map(_.named), tree)
  }

  def process(p: spl.Pipeline): LogicalPlan = p.commands.foldLeft(
      UnresolvedRelation(Seq("x")).asInstanceOf[LogicalPlan]) { (tree, command) =>
      command match {
        case spl.EvalCommand(fields) =>
          fields.foldLeft(tree) { (plan, field) =>
            val (spl.Value(name), expr) = field
            withColumn(plan, name, expr)
          }

        case spl.WhereCommand(expr) =>
          Filter(expression(expr), tree)

        case spl.SearchCommand(expr) =>
          // probably search is different from where...
          Filter(expression(expr), tree)

        case spl.TableCommand(fields) =>
          selectExpr(fields, tree)

        case spl.ConvertCommand(timeformat, convs) =>
          convs.foldLeft(tree) { (plan, fc) =>
            val name = fc.alias.getOrElse(fc.field).value
            withColumn(plan, name, spl.Call(fc.func, Seq(fc.field)))
          }

        case spl.HeadCommand(expr, keepLast, nullOption) =>
          // TODO Implement keeplast and null options behaviour
          logger.debug(s"Adding `HeadCommand` with options: ${expr} to the tree")
          if (expr.isInstanceOf[spl.IntValue])
            Limit(expression(expr), tree)
          else
            Limit(expression(expr), tree)

        case spl.SortCommand(fields) =>
          println(fields)
          val sortOrder: Seq[SortOrder] = fields.map(field => {
            val order = if (field._1.getOrElse("+") == "-") Descending else Ascending
            field._2 match {
              case spl.Call(name, args) =>
                name match {
                  case "num" =>
                    SortOrder(Cast(expression(args.head), DoubleType), order)
                  case "str" =>
                    SortOrder(Cast(expression(args.head), StringType), order)
                  case "ip" =>
                    // TODO implement logic for ip function
                    // see https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/sort
                    SortOrder(expression(args.head), order)
                  case _ =>
                    SortOrder(expression(args.head), order)
                }
              case spl.Value(value) =>
                SortOrder(expression(spl.Value(value)), order)
            }
          })
          Sort(sortOrder, global = true, tree)

        case spl.FieldsCommand(op, fields) =>
          if (op.getOrElse("+").equals("-")) {
            val fieldsToDiscard = fields.map(
              item => item.value.replace("*", "(.*)")
            ).mkString("|")

            val columnRegex = UnresolvedRegex(s"^(?!$$${fieldsToDiscard}).*$$", None, false)
            Project(Seq(columnRegex), tree)
          }
          else
            SelectExpr(fields, tree)

        case spl.SortCommand(fields) =>
          println(fields)
          val sortOrder: Seq[SortOrder] = fields.map(field => {
            val order = if (field._1.getOrElse("+") == "-") Descending else Ascending
            field._2 match {
              case spl.Call(name, args) =>
                name match {
                  case "num" =>
                    SortOrder(Cast(expression(args.head), DoubleType), order)
                  case "str" =>
                    SortOrder(Cast(expression(args.head), StringType), order)
                  case "ip" =>
                    // TODO implement logic for ip function
                    // see https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/sort
                    SortOrder(expression(args.head), order)
                  case _ =>
                    SortOrder(expression(args.head), order)
                }
              case spl.Value(value) =>
                SortOrder(expression(spl.Value(value)), order)
            }
          })
          Sort(sortOrder, global = true, tree)

        case spl.FieldsCommand(op, fields) =>
          if (op.getOrElse("+").equals("-")) {
            val fieldsToDiscard = fields.map(_.value.replace("*", "(.*)")).mkString("|")
            val columnRegex = UnresolvedRegex(s"^(?!$$${fieldsToDiscard}).*$$", None, false)
            Project(Seq(columnRegex), tree)
          }
          else
            selectExpr(fields, tree)

        case spl.LookupCommand(options, dataset, fields, output) =>
          // TODO: implement it as joins later
          tree

        case spl.CollectCommand(args, fields) =>
          //fields.map(fieldName => Column(fieldName.value))
          // TODO: add projection if fields is not empty
          AppendData(UnresolvedRelation(Seq(args("index"))), tree, Map(), isByName = true)
      }
    }

  private def withColumn(tree: LogicalPlan, name: String, expr: spl.Expr): Project = {
    output = output :+ e.Alias(expression(expr), name)()
    Project(output, tree)
  }

  private def mapCall(call: spl.Call): e.Expression = call.name match {
    case "isnull" =>
      e.IsNull(expression(call.args.head))
    case "ctime" =>
      val field = attr(call.args.head)
      Column(field).cast("date").as(field.name).named
  }

  private def expression(expr: spl.Expr): e.Expression = expr match {
    case constant: spl.Constant => mapConstants(constant)
    case call: spl.Call => mapCall(call)
    case spl.Unary(symbol, right) => symbol match {
      case spl.UnaryNot => e.Not(expression(right))
      // TODO: failure modes
    }
    case spl.Binary(left, symbol, right) => symbol match {
      case straight: spl.Straight => straight match {
        case relational: spl.Relational => relational match {
          case spl.LessThan => e.LessThan(attr(left), expression(right))
          case spl.GreaterThan =>e.GreaterThan(attr(left), expression(right))
          case spl.GreaterEquals => e.GreaterThanOrEqual(attr(left), expression(right))
          case spl.LessEquals => e.LessThanOrEqual(attr(left), expression(right))
          case spl.Equals => e.EqualTo(attr(left), expression(right))
          case spl.NotEquals => e.Not(e.EqualTo(attr(left), expression(right)))
        }
        case spl.Or => e.Or(expression(left), expression(right))
        case spl.And => e.And(expression(left), expression(right))
        // TODO: make a failure case
      }
      // TODO: make a failure case
    }
  }

  private def attr(expr: spl.Expr): UnresolvedAttribute = expr match {
    case spl.Value(value) => UnresolvedAttribute(Seq(value))
    // TODO: failure mode
  }

  private def mapConstants(constant: spl.Constant): e.Literal = constant match {
    case spl.Null() => e.Literal.create(null)
    case spl.Bool(value) => e.Literal.create(value)
    case spl.Value(value) => e.Literal.create(value)
    case spl.IntValue(value) => e.Literal.create(value)
  }
}
