package org.apache.spark.sql

import scala.collection.mutable
import scala.util.matching.Regex
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRegex, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectSet, Count, Sum, First, Last, Max, Min}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DoubleType, LongType, MetadataBuilder, StringType}

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

object SplToCatalyst extends Logging {
  def pipeline(ctx: LogicalContext, p: spl.Pipeline): LogicalPlan = {
    val (table, pipe) = determineTable(ctx, p)
    pipe.commands.foldLeft(table) {
      wrapCommand(ctx) {
        (tree, command) => command match {
          case spl.SearchCommand(expr) =>
            // probably search is different from where...
            Filter(expression(expr), tree)

          case spl.WhereCommand(expr) =>
            Filter(expression(expr), tree)

          case spl.EvalCommand(fields) =>
            fields.foldLeft(tree) { (plan, field) =>
              val (spl.Field(name), expr) = field
              withColumn(ctx, plan, name, expr)
            }

          case spl.TableCommand(fields) =>
            selectExpr(fields, tree)

          case spl.ConvertCommand(timeformat, convs) =>
            convs.foldLeft(tree) { (plan, fc) =>
              val name = fc.alias.getOrElse(fc.field).value
              withColumn(ctx, plan, name, spl.Call(fc.func, Seq(fc.field)))
            }

          case spl.HeadCommand(expr, keepLast, nullOption) =>
            // TODO Implement keeplast and null options behaviour
            logger.debug(s"Adding `HeadCommand` with options: $expr to the tree")
            if (expr.isInstanceOf[spl.IntValue]) Limit(expression(expr), tree)
            else Filter(expression(expr), tree)

          case spl.SortCommand(fields) =>
            Sort(sortOrder(fields), global = true, tree)

          case spl.FieldsCommand(op, fields) =>
            if (op.getOrElse("+").equals("-")) {
              val fieldsToDiscard = fields.map(_.value.replace("*", "(.*)")).mkString("|")
              val columnRegex = UnresolvedRegex(s"(?!$fieldsToDiscard).*", None, caseSensitive = false)
              // TODO: change LogicalContext
              Project(Seq(columnRegex), tree)
            } else selectExpr(fields, tree)

          case spl.LookupCommand(dataset, fields, output) =>
            leftJoinUsing(dataset, fields, output, tree)

          case spl.CollectCommand(args, fields) =>
            // fields.map(fieldName => Column(fieldName.value))
            // TODO: add projection if fields is not empty
            AppendData(UnresolvedRelation(Seq(args("index"))), tree, Map(), isByName = true)

          case spl.StatsCommand(params, funcs, by, dedupSplitVals) =>
            val agg = aggregate(ctx, by, funcs, tree)
            if (dedupSplitVals) {
              Deduplicate(by.map(x => UnresolvedAttribute(x.value)), agg)
            } else agg

          case spl.RexCommand(field, maxMatch, offsetField, mode, regex) =>
            // TODO find a way to implement max_match, offset_field and mode
            rexExtract(ctx, field, maxMatch, offsetField, mode, regex, tree)

          case spl.RenameCommand(aliases) =>
            renameColumn(aliases, tree)

          case spl.RegexCommand(item, regex) =>
            item match {
              case Some(value) =>
                val catalystOp = if (value._2.contains("!")) Not else (expr: Expression) => expr
                Filter(catalystOp(RLike(Column(value._1.value).expr, Literal(regex))), tree)
              case None =>
                Filter(RLike(Column(ctx.rawFieldName).expr, Literal(regex)), tree)
            }

          case spl.JoinCommand(joinType, useTime, earlier, overwrite, max, fields, subSearch) =>
            val right = pipeline(ctx.copy(output = Seq()), subSearch)
            Join(tree, right,
              UsingJoin(joinType match {
                case "inner" => Inner
                case "left" => LeftOuter
                case "outer" => LeftOuter
                case _ => Inner
              }, fields.map(_.value)), None, JoinHint.NONE)

          case spl.ReturnCommand(count, fields) =>
            val countLimit = count match {
              case Some(item) => Literal(item.value)
              case None => Literal(1)
            }

            Limit(countLimit, Project(fields.map {
              case alias: (spl.Field, spl.Expr) =>
                // TODO: rewrite to something sensible
                Alias(Column(attr(alias._2).name).named, attr(alias._1).name)()
              case field: spl.Field =>
                Column(field.value).named
            }, tree))

          case spl.FillNullCommand(value, fields) =>
            val fieldsOpt = fields.getOrElse(Seq.empty[spl.Field]).map(_.value).toSet
            FillNullShim(value.getOrElse("0"), fieldsOpt, tree)
        }
      }
    }
  }

  private def function(call: spl.Call): Expression = call.name match {
    case "isnull" =>
      IsNull(attrOrExpr(call.args.head))
    case "if" =>
      If(attrOrExpr(call.args.head), attrOrExpr(call.args(1)),attrOrExpr(call.args(2)))
    case "ctime" =>
      val field = attr(call.args.head)
      Column(field).cast("date").as(field.name).named
    case "count" =>
      Count(call.args.map(expression))
    case "sum" =>
      Sum(attr(call.args.head))
    case "min" =>
      // TODO: would currently fail on wildcard attributes
      Min(attrOrExpr(call.args.head))
    case "max" =>
      // TODO: would currently fail on wildcard attributes
      Max(attr(call.args.head))
    case "len" =>
      // https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/TextFunctions#len.28X.29
      // This function returns the character length of a string X
      Length(attrOrExpr(call.args.head))
    case "substr" =>
      //https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/TextFunctions#substr.28X.2CY.2CZ.29
      //substr(X,Y,Z) -> Returns a substring of X, starting at the index specified by Y with the number of characters specified by Z
      val str = attrOrExpr(call.args.head)
      val pos = expression(call.args(1))
      val len = call.args.lift(2).map(expression).getOrElse(Literal(Integer.MAX_VALUE))
      Substring(str,pos,len)
    case "round" =>
      val num = attrOrExpr(call.args.head)
      val scale = call.args.lift(1).map(expression).getOrElse(Literal(0))
      Round(num, scale)
    case "TERM" =>
      Term(expression(call.args.head))
    case "values" =>
      CollectSet(attrOrExpr(call.args.head))
    case "earliest" =>
      First(attrOrExpr(call.args.head), ignoreNulls = true)
    case "latest" =>
      Last(attrOrExpr(call.args.head), ignoreNulls = true)
    case "strftime" =>
      DateFormatClass(attrOrExpr(call.args.head), Literal.create(call.args.lift(1) match {
        case Some(spl.Field(fmt)) => stftimeToDateFormat.foldLeft(fmt) {
          case (a, (b, c)) => a.replaceAll(b, c)
        }
        case _ => throw new AnalysisException(s"Invalid strftime format given")
      }))
    case _ =>
      val approx = s"${call.name}(${call.args.map(_.toString).mkString(",")})"
      throw new AnalysisException(s"Unknown SPL function: $approx")
  }

  private def isFilter(x: spl.Expr, name: String) = x match {
    case spl.Binary(spl.Field(field), spl.Equals, _) if field.equals(name) => true
    case _ => false
  }

  /** Finds indices in all of the binary nodes */
  private def findIndices(search: spl.Expr): Set[String] = search match {
    case b @ spl.Binary(_, _, spl.Field(value)) if isFilter(b, "index") => Set(value)
    case spl.Binary(left, _, right) => findIndices(left) ++ findIndices(right)
    case _ => Set()
  }

  /** Removes `index` filters, as they are lifted to the top of the tree */
  private def overwriteSplSearch(x: spl.Expr): spl.Expr = x match {
    case spl.Binary(left, spl.And, right) if isFilter(left, "index") => right
    case spl.Binary(left, spl.And, right) if isFilter(right, "index") => left
    // TODO: modify "earliest" filter
    case spl.Binary(left, symbol, right) => spl.Binary(overwriteSplSearch(left),
      symbol, overwriteSplSearch(right))
    case y: spl.Expr => y
  }

  /**
   * Index maps directly on Spark's table. Any filters on index are removed.
   * If no index is specified, the value is taken from the context
   */
  private def determineTable(ctx: LogicalContext, p: spl.Pipeline): (LogicalPlan, spl.Pipeline) = {
    val indices = p.commands.flatMap {
      case spl.SearchCommand(expr) => findIndices(expr)
      case _ => Seq()
    }
    if (indices.size > 1) {
      throw new AnalysisException(s"Only one index allowed, but got: ${indices.mkString(",")}")
    }
    val tableName = indices.headOption.getOrElse(ctx.indexName)
    val table = UnresolvedRelation(Seq(tableName)).asInstanceOf[LogicalPlan]
    (table, p.copy(commands = p.commands.map {
      case s: spl.SearchCommand => s.copy(expr = overwriteSplSearch(s.expr))
      case c: spl.Command => c
    }))
  }

  private def leftJoinUsing(dataset: String,
                            fields: Seq[spl.FieldLike],
                            output: Option[spl.LookupOutput],
                            tree: LogicalPlan): Join = {
    val hasAliases: Boolean = fields exists {
      case _: spl.Field => false
      case _: spl.Alias => true
      case _: spl.AliasedField => true
    }
    var right = UnresolvedRelation(Seq(dataset)).asInstanceOf[LogicalPlan]
    if (hasAliases || output.isDefined) {
      // TODO: modify LogicalContext output
      right = Project(fields.map(fieldOrAlias) ++ (output match {
        case Some(spl.LookupOutput(kv, outputFields)) =>
          outputFields.map(fieldOrAlias)
        case None => Seq()
      }), right)
    }
    Join(tree, right,
      UsingJoin(LeftOuter, fields.map {
        case spl.Field(fieldName) => fieldName
        case spl.AliasedField(_, alias) => alias
      }), None, JoinHint.NONE)
  }

  private def fieldOrAlias(field: spl.FieldLike): NamedExpression = field match {
    case spl.Field(fieldName) =>
      UnresolvedAttribute(fieldName)
    case spl.AliasedField(spl.Field(fieldName), alias) =>
      Alias(UnresolvedAttribute(fieldName), alias)()
    case spl.Alias(expr, alias) =>
      Alias(expression(expr), alias)()
  }

  private def aggregate(ctx: LogicalContext,
                        by: Seq[spl.Field],
                        funcs: Seq[spl.Expr],
                        tree: LogicalPlan) = {
    // TODO: select _time
    val groupBy = by.map(attr)
    val agg = aggregates(funcs)
    val child = if (hasTimeFunctions(funcs)) Sort(Seq(
        SortOrder(UnresolvedAttribute(ctx.timeFieldName), Ascending, NullsFirst, Seq.empty)
    ), global = true, tree) else tree
    val plan = Aggregate(groupBy, agg, child)
    ctx.output = Seq()
    plan
  }

  private def hasTimeFunctions(funcs: Seq[spl.Expr]) : Boolean = funcs.map {
    case spl.Call(name, _) => name
    case spl.Alias(spl.Call(name, _), _) => name
    case _ => ""
  } exists(Seq("earliest", "latest").contains(_))

  private def aggregates(funcs: Seq[spl.Expr]): Seq[NamedExpression] = funcs.map {
    case call: spl.Call =>
      Alias(function(call), call.name)()
    case spl.Alias(call: spl.Call, name) =>
      Alias(function(call), name)()
    case x: spl.Expr =>
      throw new NotImplementedError(s"cannot convert aggregate: $x")
  }

  private def withColumn(ctx: LogicalContext,
                         tree: LogicalPlan,
                         name: String,
                         expr: spl.Expr): Project =
    withColumn(ctx, tree, name, attrOrExpr(expr))

  private def withColumn(ctx: LogicalContext,
                         tree: LogicalPlan,
                         name: String,
                         expression: Expression): Project =
    selectColumn(ctx, tree, Alias(expression, name)())

  private def selectColumn(ctx: LogicalContext,
                           tree: LogicalPlan,
                           ne: NamedExpression): Project = {
    ctx.output :+= ne
    Project(ctx.output, tree)
  }

  private def wrapCommand(ctx: LogicalContext)
                         (mapper: (LogicalPlan, spl.Command) => LogicalPlan)
                         (plan: LogicalPlan, command: spl.Command): LogicalPlan = {
    try {
      mapper(plan, command) match {
        case project: Project =>
          //ctx.output = project.output
          project
        case result: LogicalPlan =>
          result
      }
    } catch {
      case NonFatal(e) =>
        val name = command.getClass.getSimpleName
          .replace("Command", "").toLowerCase
        logger.warn(s"Error in $name", e)
        UnknownPlanShim(s"Error in $name: $e", plan)
    }
  }

  private def selectExpr(fields: Seq[spl.Field], tree: LogicalPlan) = {
    // TODO: replace the logic with select(ctx, tree, ne)?...
    Project(fields.map {
      case spl.Field(value) => UnresolvedAttribute(value)
    }, tree)
  }

  private def renameColumn(aliases: Seq[spl.Alias], tree: LogicalPlan): LogicalPlan = {
    // TODO: modify LogicalContext
    val myList = new ListBuffer[NamedExpression]
    val regex = aliases.map(alias => attr(alias.expr).name).mkString("|")
    myList += UnresolvedRegex(s"(?!$regex).*", None, caseSensitive = false)
    aliases foreach {alias => {
      myList += Alias(attr(alias.expr), alias.name)()
    }}
    Project(myList, tree)
  }

  // https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/Commontimeformatvariables
  // https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
  // and fix whatever is missing...
  // UNSUPPORTED: %V and %U (week of the year), %w (weekday as decimal), %k, %s	(unix epoch), and others
  private val stftimeToDateFormat = Map(
    "%Y" -> "yyyy",
    "%y" -> "yy",
    "%m" -> "MM",
    "%b" -> "MMM",
    "%B" -> "MMMM",
    "%d" -> "dd",
    "%A" -> "EEEE",
    "%a" -> "EE",
    "%e" -> "d",
    "%j" -> "D",
    "%H" -> "HH",
    "%l" -> "hh",
    "%M" -> "mm",
    "%S" -> "ss",
    "%p" -> "a",
    "%T" -> "HH:mm:ss",
    "%Z" -> "zz",
    "%%" -> "%"
  )

  private def attrOrExpr(expr: spl.Expr): Expression = expr match {
    case spl.Field(value) => UnresolvedAttribute(Seq(value))
    case _ => expression(expr)
  }

  private def expression(expr: spl.Expr): Expression = expr match {
    case constant: spl.Constant => mapConstants(constant)
    case call: spl.Call => function(call)
    case spl.Unary(symbol, right) => symbol match {
      case spl.UnaryNot => Not(expression(right))
      // TODO: failure modes
    }
    case spl.FieldIn(field, exprs) =>
      In(UnresolvedAttribute(field), exprs.map(expression))
    case spl.Binary(left, spl.Equals, spl.Wildcard(pattern)) =>
      like(left, pattern)
    case spl.Binary(left, spl.NotEquals, spl.Wildcard(pattern)) =>
      Not(like(left, pattern))
    case spl.Binary(left, symbol, right) => symbol match {
      case straight: spl.Straight => straight match {
        case relational: spl.Relational => relational match {
          case spl.LessThan => LessThan(attr(left), expression(right))
          case spl.GreaterThan => GreaterThan(attr(left), expression(right))
          case spl.GreaterEquals => GreaterThanOrEqual(attr(left), expression(right))
          case spl.LessEquals => LessThanOrEqual(attr(left), expression(right))
          case spl.Equals => EqualTo(attr(left), expression(right))
          case spl.NotEquals => Not(EqualTo(attr(left), expression(right)))
        }
        case spl.Or => Or(expression(left), expression(right))
        case spl.And => And(expression(left), expression(right))
        case spl.Add => Add(expression(left), expression(right))
        case spl.Subtract => Subtract(expression(left), expression(right))
        case spl.Multiply => Multiply(expression(left), expression(right))
        case spl.Divide => Divide(expression(left), expression(right))
        case spl.Concatenate => Concat(Seq(expression(left), expression(right)))
        // TODO: make a failure case
      }
    }
    case _ => throw new AnalysisException(s"Cannot translate $expr")
  }

  private def like(left: spl.Expr, pattern: String): Like = {
    val regex = Literal.create(pattern.replaceAll("\\*", "%"))
    Like(attrOrExpr(left), regex, '\\')
  }

  private def attr(expr: spl.Expr): UnresolvedAttribute = expr match {
    case spl.Field(value) => UnresolvedAttribute(Seq(value))
    // TODO: failure mode
  }

  private def mapConstants(constant: spl.Constant): Literal = constant match {
    case spl.Null() => Literal.create(null)
    case spl.Bool(value) => Literal.create(value)
    case spl.Field(value) => Literal.create(value)
    case spl.StrValue(value) => Literal.create(value)
    case spl.IntValue(value) => Literal.create(value)
  }

  private def rexParseNamedGroup(inputString: String): mutable.Map[String, Int] = {
    val namedGroupPattern: Regex = "<([a-zA-Z_0-9]+)>".r
    val namedGroupMap = mutable.Map[String, Int]()
    var index = 1
    for (patternMatch <- namedGroupPattern.findAllMatchIn(inputString)) {
      namedGroupMap(patternMatch.group(1)) = index
      index += 1
    }
    namedGroupMap
  }

  private def sortOrder(fields: Seq[(Option[String], spl.Expr)]): Seq[SortOrder] = {
    fields map {
      case Tuple2(a, b) => (b, if (a.getOrElse("+") == "-") Descending else Ascending)
    } map {
      case (spl.Call(name, args), order) => name match {
        case "num" =>
          SortOrder(Cast(attr(args.head), DoubleType), order)
        case "str" =>
          SortOrder(Cast(attr(args.head), StringType), order)
        case "ip" =>
          // TODO implement logic for ip function
          // see https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/sort
          SortOrder(attr(args.head), order)
        case _ =>
          SortOrder(attr(args.head), order)
      }
      case (spl.Field(value), order) =>
        SortOrder(UnresolvedAttribute(value), order)
    }
  }

  private def rexExtract(ctx: LogicalContext,
                         field: Option[String],
                         maxMatch: Int,
                         offsetField: Option[String],
                         mode: Option[String],
                         regex: String,
                         tree: LogicalPlan): LogicalPlan = {
    mode match {
      case Some(value) =>
        throw new NotImplementedError(s"rex mode=$value currently not supported!")
      case None =>
        val raw = selectColumn(ctx, tree, UnresolvedAttribute(ctx.rawFieldName))
        rexParseNamedGroup(regex).foldLeft(raw) {
          case (plan, (colName, groupIndex)) =>
            withColumn(ctx, plan, colName, RegExpExtract(field match {
              case Some(value) => UnresolvedAttribute(value)
              case None => UnresolvedAttribute(ctx.rawFieldName)
            }, Literal(regex), Literal(groupIndex)))
        }
    }
  }
}

case class UnknownPlanShim(t: String, child: LogicalPlan) extends LogicalPlan {
  override def output: Seq[Attribute] = child.output
  override def children: Seq[LogicalPlan] = child.children
}