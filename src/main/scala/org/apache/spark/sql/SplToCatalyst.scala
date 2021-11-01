package org.apache.spark.sql

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.control.NonFatal
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, UsingJoin}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}

object SplToCatalyst extends Logging {
  def pipeline(ctx: LogicalContext, p: spl.Pipeline): LogicalPlan = {
    val (table, pipe) = determineTable(ctx, p)
    pipe.commands.foldLeft(table) {
      wrapCommand(ctx) {
        (tree, command) => command match {
          case spl.SearchCommand(expr) =>
            // probably search is different from where...
            Filter(expression(expr), tree)

          case inputLookup: spl.InputLookup =>
            // TODO implement `append`, `strict` and `start` options, also inputlookup should accept file name
            applyInputLookup(ctx, tree, inputLookup)

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
            log.debug(s"Adding `HeadCommand` with options: $expr to the tree")
            if (expr.isInstanceOf[spl.IntValue]) Limit(expression(expr), tree)
            else Filter(expression(expr), tree)

          case spl.SortCommand(fields) =>
            Sort(sortOrder(fields), global = true, tree)

          case spl.FieldsCommand(rmFields, fields) =>
            applyFields(ctx, tree, rmFields, fields)

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

          case rc: spl.RexCommand =>
            // TODO find a way to implement max_match, offset_field and mode
            applyRex(ctx, tree, rc)

          case spl.RenameCommand(aliases) =>
            applyRename(ctx, tree, aliases)

          case spl.RegexCommand(item, regex) =>
            applyRegex(ctx, tree, item, regex)

          case jc: spl.JoinCommand =>
            applyJoin(ctx, tree, jc)

          case spl.ReturnCommand(count, fieldsOrAliases) =>
            applyReturn(ctx, tree, count, fieldsOrAliases)

          case spl.FillNullCommand(value, fields) =>
            val fieldsOpt = fields.getOrElse(Seq.empty[spl.Field]).map(_.value).toSet
            FillNullShim(value.getOrElse("0"), fieldsOpt, tree)

          case spl.EventStatsCommand(params, funcs, by) =>
            // TODO implement allnum option
            applyEventStats(ctx, tree, params, funcs, by)

          case dedupCmd: spl.DedupCommand =>
            applyDedup(ctx, tree, dedupCmd)
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
      // TODO: if count is empty, make the Count(Literal.create(1))
      Count(call.args.map(expression))
    case "sum" =>
      Sum(attr(call.args.head))
    case "min" =>
      // TODO: would currently fail on wildcard attributes
      AggregateExpression(
        Min(attrOrExpr(call.args.head)),
        Complete, isDistinct = false)
    case "max" =>
      // TODO: would currently fail on wildcard attributes
      AggregateExpression(
        Max(attr(call.args.head)),
        Complete, isDistinct = false)
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
    case "coalesce" =>
      Coalesce(call.args.map(attrOrExpr))
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
      case inputLookup: spl.InputLookup => Set(inputLookup.tableName)
      case _ => Seq()
    }
    if (indices.size > 1) {
      throw new AnalysisException(s"Only one index allowed, but got: ${indices.mkString(",")}")
    }
    val tableName = indices.headOption.getOrElse(ctx.indexName)
    val table = UnresolvedRelation(Seq(tableName)).asInstanceOf[LogicalPlan]
    ctx.output ++= ctx.analyzePlan(table)
    // `filterNot` is being used here to filter out a `SearchCommand` that only contains `index`.
    // The idea is to remove the `index` filters, as they are lifted to the top of the tree
    (table, p.copy(commands = p.commands.filterNot {
        case s: spl.SearchCommand => isFilter(s.expr, "index")
        case _ => false
      }.map {
        case s: spl.SearchCommand => s.copy(expr = overwriteSplSearch(s.expr))
        case c: spl.Command => c
      }
    ))
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
    ctx.output = Seq()
    // new Aggregate(groupBy, agg, child) translates to
    // INVOKESPECIAL Aggregate.<init> (LSeq;Seq;LLogicalPlan;)V
    // and it's not filling in default arguments
    newAggregateIgnoringABI(groupBy, agg, child)
  }

  // we're using Spark Catalyst's private APIs, so there are absolutely no guarantees
  // on binary compatibility of different Spark implementations. This is a hack to
  // make one of the most important SPL commands to be runnable in Databricks Runtime.
  private def newAggregateIgnoringABI(groupingExpressions: Seq[Expression],
              aggregateExpressions: Seq[NamedExpression],
              child: LogicalPlan): Aggregate = aggregateConstructorArgCountABI match {
    case 3 => Aggregate(groupingExpressions, aggregateExpressions, child)
    case 4 => aggregateConstructorReflection
      .newInstance(groupingExpressions, aggregateExpressions, child, None)
      .asInstanceOf[Aggregate]
    case _ => throw new AnalysisException(s"Incompatible runtime detected")
  }

  private val (aggregateConstructorReflection, aggregateConstructorArgCountABI) = {
    val firstConstructor = classOf[Aggregate].getConstructors.head
    (firstConstructor, firstConstructor.getParameterCount)
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
    // To avoid duplicate column
    if (!ctx.output.map(_.name).contains(ne.name))
      ctx.output :+= ne
    Project(ctx.output, tree)
  }

  private def selectColumns(ctx: LogicalContext,
                            tree: LogicalPlan,
                            namedExprs: Seq[NamedExpression]): Project = {
    ctx.output = namedExprs
    Project(ctx.output, tree)
  }

  private def wrapCommand(ctx: LogicalContext)
                         (mapper: (LogicalPlan, spl.Command) => LogicalPlan)
                         (plan: LogicalPlan, command: spl.Command): LogicalPlan = {
    try {
      mapper(plan, command) match {
        case project: Project =>
          ctx.output = project.output
          project
        case result: LogicalPlan =>
          result
      }
    } catch {
      case NonFatal(e) =>
        val name = command.getClass.getSimpleName
          .replace("Command", "").toLowerCase
        log.warn(s"Error in $name", e)
        UnknownPlanShim(s"Error in $name: $e", plan)
    }
  }

  private def selectExpr(fields: Seq[spl.Field], tree: LogicalPlan) = {
    // TODO: replace the logic with select(ctx, tree, ne)?...
    Project(fields.map {
      case spl.Field(value) => UnresolvedAttribute(value)
    }, tree)
  }

  private def applyRename(ctx: LogicalContext, tree: LogicalPlan, aliases: Seq[spl.Alias]): LogicalPlan = {
    val aliasMap = aliases.map(a => (attr(a.expr).name -> a)).toMap
    Project(ctx.output.map(item =>
      aliasMap.get(item.name) match {
        case Some(alias) => Alias(attr(alias.expr), alias.name)()
        case _ => item
      }
    ), tree)
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

  private def applyFields(ctx: LogicalContext,
                          tree: LogicalPlan,
                          removeFields: Boolean,
                          fields: Seq[spl.Field]): LogicalPlan =
    if (removeFields) {
      val columns = fields.map(_.value)
      selectColumns(ctx, tree, ctx.output.filter(item => !columns.contains(item.name)))
    }
    else
      selectExpr(fields, tree)

  private def applyRex(ctx: LogicalContext, tree: LogicalPlan, rc: spl.RexCommand): LogicalPlan =
  // TODO find a way to implement max_match, offset_field and mode
    rc.mode match {
      case Some(value) =>
        throw new NotImplementedError(s"rex mode=$value currently not supported!")
      case None =>
        val raw = selectColumn(ctx, tree, UnresolvedAttribute(ctx.rawFieldName))
        rexParseNamedGroup(rc.regex).foldLeft(raw) {
          case (plan, (colName, groupIndex)) =>
            withColumn(ctx, plan, colName, RegExpExtract(rc.field match {
              case Some(value) => UnresolvedAttribute(value)
              case None => UnresolvedAttribute(ctx.rawFieldName)
            }, Literal(rc.regex), Literal(groupIndex)))
        }
    }

  private def applyJoin(ctx: LogicalContext, tree: LogicalPlan, jc: spl.JoinCommand) = {
    val right = pipeline(ctx.copy(output = Seq()), jc.subSearch)
    Join(tree, right,
      UsingJoin(jc.joinType match {
        case "inner" => Inner
        case "left" => LeftOuter
        case "outer" => LeftOuter
        case _ => Inner
      }, jc.fields.map(_.value)), None, JoinHint.NONE)
  }

  private def applyRegex(ctx: LogicalContext,
                         tree: LogicalPlan,
                         item: Option[(spl.Field, String)],
                         regex: String) =
    item match {
      case Some((spl.Field(field), exclude)) =>
        val catalystOp = if (exclude.contains("!")) Not else (expr: Expression) => expr
        Filter(catalystOp(RLike(Column(field).expr, Literal(regex))), tree)
      case None =>
        Filter(RLike(Column(ctx.rawFieldName).expr, Literal(regex)), tree)
    }

  private def applyReturn(ctx: LogicalContext,
                          tree: LogicalPlan,
                          count: spl.IntValue,
                          fieldsOrAliases: Seq[spl.FieldOrAlias]) = {
    Limit(Literal(count.value), Project(fieldsOrAliases.map {
      case field: spl.Field => UnresolvedAttribute(field.value)
      case alias: spl.Alias => Alias(attr(alias.expr), alias.name)()
    }, tree))
  }

  private def applyEventStats(ctx: LogicalContext,
                              tree: LogicalPlan,
                              params: Map[String, String],
                              funcs: Seq[spl.Expr],
                              by: Seq[spl.Field] = Seq()): LogicalPlan = {
    // TODO implement allnum option
    val partitionSpec = by.map(attr)
    val sortOrderSpec = sortOrder(by.map(field => (Some("+"), field)))
    val windowSpec = WindowSpecDefinition(partitionSpec, sortOrderSpec, UnspecifiedFrame)
    funcs.foldLeft(tree) {
      case (plan, spl.Alias(expr, name)) =>
        withColumn(ctx, plan, name, WindowExpression(expression(expr), windowSpec))
      case (plan, expr: spl.Expr) =>
        withColumn(ctx, plan,
          // when no name/alias is passed to the spl command, spl generates default column name based on the expression
          // ie. `eventstats min(column) ...` will create a column named "min(column)"
          expression(expr).toString.replace("'", ""),
          WindowExpression(expression(expr), windowSpec))
    }
  }

  /**
   * TODO: For sorting we need to check that the Lexicographical order in Splunk is the same in Spark
   * https://docs.splunk.com/Documentation/Splunk/8.2.2/SearchReference/Dedup
   */
  private def getSortByWindowExpr(fields: Seq[spl.Field], cmd: spl.SortCommand): WindowExpression = {
    WindowExpression(RowNumber(), WindowSpecDefinition(
      partitionSpec = fields.map(attr),
      orderSpec = sortOrder(cmd.fieldsToSort),
      frameSpecification = UnspecifiedFrame)
    )
  }

  private def applyDedup(ctx: LogicalContext, tree: LogicalPlan, cmd: spl.DedupCommand) = {
    val windowExpr = getSortByWindowExpr(cmd.fields, cmd.sortBy)
    Project(ctx.output, Filter(
      LessThanOrEqual(UnresolvedAttribute("_rn"), Literal(cmd.numResults.value)),
      withColumn(ctx,
        withColumn(ctx, tree, "_no", MonotonicallyIncreasingID()),
        "_rn", windowExpr)
      )
    )
  }

  private def applyInputLookup(ctx: LogicalContext, plan: LogicalPlan, iLookup: spl.InputLookup) = {
    // TODO implement `append`, `strict` and `start` options, also inputlookup should accept file name
    Limit(
      Literal(iLookup.max.value),
      iLookup.where match {
        case Some(where) => Filter(expression(where.expr), plan)
        case _ => plan
      })
  }
}

case class UnknownPlanShim(t: String, child: LogicalPlan) extends LogicalPlan {
  override def output: Seq[Attribute] = child.output
  override def children: Seq[LogicalPlan] = child.children
}