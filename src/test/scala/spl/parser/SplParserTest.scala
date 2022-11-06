package spl.parser



class SplParserTest extends ParserSuite {
  // scalastyle:off
  // turn it back on once figure out how to force IDE not to reformat
  import spl.parser.SplParser._
  import spl.ast._

  test("debugging") {
    import fastparse._
    import fastparse.MultiLineWhitespace._
    // this method tests debug logger, that is supposed
    // to troubleshoot broken parsers
    def te[_: P]: P[String] = ("a" ~ token).log.@@
    parses("a b", te(_), "b")
  }

  test("debugging behaviour when logging is not enabled") {
    import fastparse._
    import fastparse.MultiLineWhitespace._
    def te[_: P]: P[String] = ("A" ~ token).log.@@
    assert(
      parseInputRaw("A B", te(_), enableLogging = false).successValue == "B"
    )
  }

  test("more debugging") {
    import fastparse._
    import fastparse.MultiLineWhitespace._

    def tap[_: P]: P[String] = ("a" ~ token).log.tap(r => r.toString)
    parses("a b", tap(_), "b")
  }

  test("false") {
    p(bool(_), Bool(false))
  }

  test("f") {
    p(bool(_), Bool(false))
  }

  test("from") {
    p(expr(_), Field("from"))
  }

  test("true") {
    p(bool(_), Bool(true))
  }

  test("t") {
    p(bool(_), Bool(true))
  }

  test("tree") {
    p(expr(_), Field("tree"))
  }

  test("left") {
    p(field(_), Field("left"))
  }

  test("foo   = bar") {
    p(fieldAndValue(_), FV("foo", "bar"))
  }

  test("foo=bar bar=baz") {
    p(commandOptions(_), CommandOptions(Seq(
      FC("foo", Field("bar")),
      FC("bar", Field("baz"))
    )))
  }

  test("a ,   b,c, d") {
    p(fieldList(_), Seq(
      Field("a"),
      Field("b"),
      Field("c"),
      Field("d")
    ))
  }

  test("D:\\Work\\Stuff.xls") {
    p(filename(_), "D:\\Work\\Stuff.xls")
  }

  test("-100500") {
    p(int(_), IntValue(-100500))
  }

  test("1sec") {
    p(timeSpan(_), TimeSpan(1, "seconds"))
  }

  test("5s") {
    p(timeSpan(_), TimeSpan(5, "seconds"))
  }

  test("5second") {
    p(timeSpan(_), TimeSpan(5, "seconds"))
  }

  test("5sec") {
    p(timeSpan(_), TimeSpan(5, "seconds"))
  }

  test("5m") {
    p(timeSpan(_), TimeSpan(5, "minutes"))
  }

  test("5mins") {
    p(timeSpan(_), TimeSpan(5, "minutes"))
  }

  test("-5mon") {
    p(timeSpan(_), TimeSpan(-5, "months"))
  }

  test("-5d@w-2h") {
    p(constant(_), SnapTime(
      Some(TimeSpan(-5, "days")),
      "weeks",
      Some(TimeSpan(-2, "hours"))))
  }

  test("-5d@w0-2h") {
    p(constant(_), SnapTime(
      Some(TimeSpan(-5, "days")),
      "weeks",
      Some(TimeSpan(-2, "hours"))))
  }

  test("-5d@w") {
    p(constant(_), SnapTime(
      Some(TimeSpan(-5, "days")),
      "weeks",
      None))
  }

  test("@w") {
    p(constant(_), SnapTime(None, "weeks", None))
  }

  test("@w-1d") {
    p(constant(_), SnapTime(None, "weeks", Some(TimeSpan(-1, "days"))))
  }

  test("-1h@h") {
    p(constant(_), SnapTime(Some(TimeSpan(-1, "hours")), "hours", None))
  }

  test("-h@h") {
    p(constant(_), SnapTime(Some(TimeSpan(-1, "hours")), "hours", None))
  }

  test("h@h") {
    p(constant(_), SnapTime(Some(TimeSpan(1, "hours")), "hours", None))
  }

  test("a=b c=1 d=\"e\" f=g* h=-15m i=10.0.0.0/8 k=f") {
    p(commandOptions(_), CommandOptions(Seq(
      FC("a", Field("b")),
      FC("c", IntValue(1)),
      FC("d", StrValue("e")),
      FC("f", Wildcard("g*")),
      FC("h", TimeSpan(-15, "minutes")),
      FC("i", IPv4CIDR("10.0.0.0/8")),
      FC("k", Bool(false))
    )))
  }

  test("a OR b") {
    p(expr(_), Binary(
      Field("a"),
      Or,
      Field("b")
    ))
  }

  // TODO: add wildcard AST transformation
  test("productID=\"S*G01\"") {
    p(expr(_), Binary(
      Field("productID"),
      Equals,
      Wildcard("S*G01")
    ))
  }

  test("(event_id=12 OR event_id=13 OR event_id=14)") {
    p(expr(_), Binary(
      Binary(
        Field("event_id"),
        Equals,
        IntValue(12)
      ),
      Or,
      Binary(
        Binary(
          Field("event_id"),
          Equals,
          IntValue(13)
        ),
        Or,
        Binary(
          Field("event_id"),
          Equals,
          IntValue(14)
        )
      )
    ))
  }

  test("a=b b=c (c=f OR d=t)") {
    p(impliedSearch(_), SearchCommand(Binary(
      Binary(
        Binary(
          Field("a"),
          Equals,
          Field("b")
        ),
        And,
        Binary(
          Field("b"),
          Equals,
          Field("c")
        )
      ),
      And,
      Binary(
        Binary(
          Field("c"),
          Equals,
          Bool(false)
        ),
        Or,
        Binary(
          Field("d"),
          Equals,
          Bool(true)
        )
      )
    )))
  }

  test("code IN(4*, 5*)") {
    p(impliedSearch(_), SearchCommand(
      FieldIn("code", Seq(
        Wildcard("4*"),
        Wildcard("5*")
      ))))
  }

  test("var_5 IN (str_2 str_3)") {
    p(impliedSearch(_), SearchCommand(
      FieldIn("var_5", Seq(
        Field("str_2"),
        Field("str_3")
      ))))
  }

  test("NOT code IN(4*, 5*)") {
    p(impliedSearch(_), SearchCommand(
      Unary(UnaryNot,
        FieldIn("code", Seq(
          Wildcard("4*"),
          Wildcard("5*"))))
    ))
  }

  test("code IN(10, 29, 43) host!=\"localhost\" xqp>5") {
    p(impliedSearch(_), SearchCommand(
      Binary(
        Binary(
          FieldIn("code", Seq(
            IntValue(10),
            IntValue(29),
            IntValue(43))),
          And,
          Binary(
            Field("host"),
            NotEquals,
            StrValue("localhost")
          )
        ),
        And,
        Binary(
          Field("xqp"),
          GreaterThan,
          IntValue(5)
        )
      )
    ))
  }

  test("head 20") {
    p(head(_),
      HeadCommand(
        IntValue(20),
        Bool(false),
        Bool(false)
      )
    )
  }

  test("head limit=400") {
    p(head(_),
      HeadCommand(
        IntValue(400),
        Bool(false),
        Bool(false))
    )
  }

  test("head limit=400 keeplast=true null=false") {
    p(head(_),
      HeadCommand(
        IntValue(400),
        Bool(true),
        Bool(false)
      )
    )
  }

  test("head count>10") {
    p(head(_),
      HeadCommand(
        Binary(
          Field("count"),
          GreaterThan,
          IntValue(10)
        ),
        Bool(false),
        Bool(false)
      )
    )
  }

  test("fields column_a, column_b, column_c") {
    p(fields(_),
      FieldsCommand(
        removeFields = false,
        Seq(
          Field("column_a"),
          Field("column_b"),
          Field("column_c")
        )
      )
    )
  }

  test("fields + column_a, column_b") {
    p(fields(_),
      FieldsCommand(
        removeFields = false,
        Seq(
          Field("column_a"),
          Field("column_b")
        )
      )
    )
  }

  test("fields - column_a, column_b") {
    p(fields(_),
      FieldsCommand(
        removeFields = true,
        Seq(
          Field("column_a"),
          Field("column_b")
        )
      )
    )
  }

  test("sort A, -B, +num(C)") {
    p(sort(_),
      SortCommand(
        Seq(
          (None, Field("A")),
          (Some("-"), Field("B")),
          (Some("+"), Call("num", Seq(Field("C"))))
        )
      )
    )
  }

  test("TERM(XXXXX*\\\\XXXXX*)") {
    p(pipeline(_), Pipeline(Seq(
      SearchCommand(
        Call("TERM", List(Field("XXXXX*\\\\XXXXX*")))
      )
    )))
  }

  test("values(eval(mvappend(\"a: \" . a, \"b: \" . b)))") {
    p(pipeline(_), Pipeline(Seq(
      SearchCommand(
        Call("values", Seq(
          Call("eval", Seq(
            Call("mvappend",
              Seq(
                Binary(
                  StrValue("a: "),
                  Concatenate,
                  Field("a")
                ),
                Binary(
                  StrValue("b: "),
                  Concatenate,
                  Field("b")
                )))))))))))
  }

  test("sort A") {
    p(sort(_),
      SortCommand(
        Seq(
          (None, Field("A"))
        )
      )
    )
  }

  test("eval mitre_category=\"Discovery\"") {
    p(eval(_), EvalCommand(Seq(
      (Field("mitre_category"), StrValue("Discovery"))
    )))
  }

  test("eval email_lower=lower(email)") {
    p(eval(_), EvalCommand(Seq(
      (Field("email_lower"), Call("lower", Seq(Field("email"))))
    )))
  }

  test("eval replaced=replace(email, \"@.+\", \"\")") {
    p(eval(_), EvalCommand(Seq(
      (Field("replaced"),
        Call("replace", Seq(Field("email"), StrValue("@.+"), StrValue(""))))
    )))
  }

  test("eval hash_sha256= lower(hash_sha256), b=c") {
    p(eval(_), EvalCommand(Seq(
      (Field("hash_sha256"), Call("lower", Seq(Field("hash_sha256")))),
      (Field("b"), Field("c"))
    )))
  }

  test("convert ctime(indextime)") {
    p(convert(_), ConvertCommand(convs = Seq(
      FieldConversion("ctime", Field("indextime"), None)
    )))
  }

  test("collect index=threathunting addtime=f x, y,  z") {
    p(pipeline(_), Pipeline(Seq(
      CollectCommand(
        index = "threathunting",
        fields = Seq(
          Field("x"),
          Field("y"),
          Field("z")
        ),
        addTime = false,
        file = null,
        host = null,
        marker = null,
        outputFormat = "raw",
        runInPreview = false,
        spool = true,
        source = null,
        sourceType = null,
        testMode = false
      )
    )))
  }

  test("index=foo bar=baz | eval foo=bar | collect index=newer") {
    p(pipeline(_), Pipeline(Seq(
      SearchCommand(
        Binary(
          Binary(
            Field("index"),
            Equals,
            Field("foo")
          ),
          And,
          Binary(
            Field("bar"),
            Equals,
            Field("baz")
          )
        )
      ),
      EvalCommand(Seq(
        (Field("foo"),Field("bar"))
      )),
      CollectCommand(
        index = "newer",
        fields = Seq(),
        addTime = true,
        file = null,
        host = null,
        marker = null,
        outputFormat = "raw",
        runInPreview = false,
        spool = true,
        source = null,
        sourceType = null,
        testMode = false
      )
    )))
  }

  test("lookup process_create_whitelist a b output reason") {
    p(pipeline(_), Pipeline(Seq(
      LookupCommand(
        "process_create_whitelist",
        Seq(
          Field("a"),
          Field("b")
        ),
        Some(
          LookupOutput(
            "output",
            Seq(
              Field("reason")
            )
          )
        )
      )
    )))
  }

  test("where isnull(reason)") {
    p(pipeline(_), Pipeline(Seq(
      WhereCommand(
        Call(
          "isnull",Seq(
            Field("reason")
          )
        )
      )
    )))
  }

  test("table foo bar baz*") {
    p(pipeline(_), Pipeline(Seq(
      TableCommand(Seq(
        Field("foo"),
        Field("bar"),
        Field("baz*")
      ))
    )))
  }

  test("stats first(startTime) AS startTime, last(histID) AS lastPassHistId BY testCaseId") {
    p(pipeline(_), Pipeline(Seq(
      StatsCommand(
        partitions = 1,
        allNum = false,
        delim = " ",
        funcs = Seq(
          Alias(
            Call("first", Seq(
              Field("startTime")
            )),
            "startTime"),
          Alias(
            Call("last", Seq(
              Field("histID")
            )),
            "lastPassHistId")
        ),
        by = Seq(
          Field("testCaseId")
        ))
    )))
  }

  test("stats count(eval(status=404))") {
    p(pipeline(_), Pipeline(Seq(
      StatsCommand(
        partitions = 1,
        allNum = false,
        delim = " ",
        funcs = Seq(
          Call("count", Seq(
            Call("eval", Seq(
              Binary(
                Field("status"),
                Equals,
                IntValue(404)
              )
            ))
          ))
        )
      ))
    ))
  }

  test("no-comma stats") {
    val query =
      """stats allnum=f delim=":" partitions=10 count
        |earliest(_time) as earliest latest(_time) as latest
        |values(var_2) as var_2
        |by var_1
        |""".stripMargin
    parses(query, stats(_), StatsCommand(
      partitions = 10,
      allNum = false,
      delim = ":",
      Seq(
        Call("count"),
        Alias(Call("earliest", Seq(Field("_time"))), "earliest"),
        Alias(Call("latest", Seq(Field("_time"))), "latest"),
        Alias(Call("values", Seq(Field("var_2"))), "var_2")
      ),
      Seq(
        Field("var_1")
      )
    ))
  }

  test("rex field=savedsearch_id max_match=10 " +
    "\"(?<user>\\w+);(?<app>\\w+);(?<SavedSearchName>\\w+)\"") {
    p(pipeline(_), Pipeline(Seq(
      RexCommand(
        Some("savedsearch_id"),
        10,
        None,
        None,
        "(?<user>\\w+);(?<app>\\w+);(?<SavedSearchName>\\w+)"
      )
    )))
  }

  test("rex mode=sed \"s/(\\d{4}-){3}/XXXX-XXXX-XXXX-/g\"") {
    p(pipeline(_), Pipeline(Seq(
      RexCommand(
        None,
        1,
        None,
        Some("sed"),
        "s/(\\d{4}-){3}/XXXX-XXXX-XXXX-/g"
      )
    )))
  }

  test("rename _ip AS IPAddress") {
    p(rename(_),
      RenameCommand(
        Seq(Alias(
          Field("_ip"),
          "IPAddress"
        )))
    )
  }

  test("rename _ip AS IPAddress, _host AS host, _port AS port") {
    p(rename(_),
      RenameCommand(Seq(
        Alias(
          Field("_ip"),
          "IPAddress"
        ),
        Alias(
          Field("_host"),
          "host"
        ),
        Alias(
          Field("_port"),
          "port"
        )))
    )
  }

  // Regex not taken into account
  test("rename foo* AS bar*") {
    p(rename(_),
      RenameCommand(
        Seq(Alias(
          Field("foo*"),
          "bar*"
        )))
    )
  }

  test("rename count AS \"Count of Events\"") {
    p(rename(_),
      RenameCommand(
        Seq(Alias(
          Field("count"),
          "Count of Events"
        )))
    )
  }

  test("join product_id [search vendors]") {
    p(join(_),
      JoinCommand(
        joinType = "inner",
        useTime = false,
        earlier = true,
        overwrite = false,
        max = 1,
        Seq(Field("product_id")),
        Pipeline(Seq(
          SearchCommand(Field("vendors"))))
      )
    )
  }

  test("join type=left usetime=true earlier=false " +
    "overwrite=false product_id, host, name [search vendors]") {
    p(join(_),
      JoinCommand(
        joinType = "left",
        useTime = true,
        earlier = false,
        overwrite = false,
        max = 1,
        Seq(
          Field("product_id"),
          Field("host"),
          Field("name")
        ),
        Pipeline(Seq(
          SearchCommand(Field("vendors"))))
      )
    )
  }

  test("join product_id [search vendors | rename pid AS product_id]") {
    p(join(_),
      JoinCommand(
        joinType = "inner",
        useTime = false,
        earlier = true,
        overwrite = false,
        max = 1,
        Seq(Field("product_id")),
        Pipeline(Seq(
          SearchCommand(Field("vendors")),
          RenameCommand(Seq(
            Alias(
              Field("pid"),
              "product_id"
            )))
        ))
      )
    )
  }

  test("regex _raw=\"(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)\"") {
    p(_regex(_), RegexCommand(
      Some((Field("_raw"), "=")),
      "(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)"))
  }

  test("regex _raw!=\"(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)\"") {
    p(_regex(_), RegexCommand(
      Some((Field("_raw"), "!=")),
      "(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)"))
  }

  test("regex \"(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)\"") {
    p(_regex(_), RegexCommand(
      None,
      "(?<!\\d)10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(?!\\d)"))
  }

  test("return 10 $test $env") {
    p(_return(_), ReturnCommand(
      IntValue(10),
      Seq(
        Field("test"),
        Field("env")
      )
    ))
  }

  test("return 10 ip src host port") {
    p(_return(_), ReturnCommand(
      IntValue(10),
      Seq(
        Field("ip"),
        Field("src"),
        Field("host"),
        Field("port")
      )
    ))
  }

  test("return 10 ip=src host=port") {
    p(_return(_), ReturnCommand(
      IntValue(10),
      Seq(
        Alias(Field("src"), "ip"),
        Alias(Field("port"), "host")
      )
    ))
  }

  test("fillnull") {
    p(fillNull(_), FillNullCommand(None, None))
  }

  test("fillnull value=NA") {
    p(fillNull(_), FillNullCommand(Some("NA"), None))
  }

  test("fillnull value=\"NULL\" host port") {
    p(fillNull(_), FillNullCommand(
      Some("NULL"),
      Some(Seq(
        Field("host"),
        Field("port")
      ))))
  }

  test("dedup 10 keepevents=true keepempty=false consecutive=true host ip port") {
    p(dedup(_), DedupCommand(
      10,
      Seq(
        Field("host"),
        Field("ip"),
        Field("port")
      ),
      keepEvents = true,
      keepEmpty = false,
      consecutive = true,
      SortCommand(Seq((Some("+"), Field("_no"))))
    ))
  }

  test("dedup 10 keepevents=true host ip port sortby +host -ip") {
    p(dedup(_), DedupCommand(
      10,
      Seq(
        Field("host"),
        Field("ip"),
        Field("port")
      ),
      keepEvents = true,
      keepEmpty = false,
      consecutive = false,
      SortCommand(
        Seq(
          (Some("+"), Field("host")),
          (Some("-"), Field("ip"))
        )
      )
    ))
  }

  test("inputlookup append=t strict=f myTable where test_id=11") {
    p(inputLookup(_), InputLookup(
      append = true,
      strict = false,
      start = 0,
      max = 1000000000,
      "myTable",
      Some(
        Binary(
          Field("test_id"),
          Equals,
          IntValue(11)
        )
    )))
  }

  test("inputlookup myTable") {
    p(inputLookup(_), InputLookup(
      append = false,
      strict = false,
      start = 0,
      max = 1000000000,
      "myTable",
      None
    ))
  }

  test("format maxresults=10") {
    p(format(_), FormatCommand(
      mvSep = "OR",
      maxResults = 10,
      rowPrefix = "(",
      colPrefix =  "(",
      colSep = "AND",
      colEnd = ")",
      rowSep = "OR",
      rowEnd = ")"
    ))
  }

  test("format mvsep=\"||\" \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"") {
    p(format(_), FormatCommand(
      mvSep = "||",
      maxResults = 0,
      rowPrefix = "[",
      colPrefix = "[",
      colSep = "&&",
      colEnd = "]",
      rowSep = "||",
      rowEnd = "]"
    ))
  }

  test("mvcombine host") {
    p(mvcombine(_), MvCombineCommand(
      None,
      Field("host")
    ))
  }

  test("mvcombine delim=\",\" host") {
    p(mvcombine(_), MvCombineCommand(
      Some(","),
      Field("host")
    ))
  }

  test("bin span=30m minspan=5m bins=20 start=0 end=20 aligntime=latest foo AS bar") {
    p(command(_), BinCommand(
      Alias(Field("foo"), "bar"),
      Some(TimeSpan(30, "minutes")),
      Some(TimeSpan(5, "minutes")),
      Some(20),
      Some(0),
      Some(20),
      Some("latest")))
  }

  test("makeresults") {
    p(command(_), MakeResults(
      count = 1,
      annotate = false,
      server = "local",
      serverGroup = null))
  }

  test("makeresults count=10 annotate=t splunk_server_group=group0") {
    p(command(_), MakeResults(
      count = 10,
      annotate = true,
      server = "local",
      serverGroup = "group0"))
  }

  test("addtotals row=t col=f fieldname=num_total num_1 num_2") {
    p(command(_), AddTotals(
      fields = Seq(Field("num_1"), Field("num_2")),
      row = true,
      col = false,
      fieldName = "num_total",
      labelField = null,
      label = "Total"
    ))
  }

  test("eventstats min(n) by gender") {
    p(command(_), EventStatsCommand(
      allNum = false,
      funcs = Seq(
        Call("min", Seq(Field("n")))
      ),
      by = Seq(Field("gender"))
    ))
  }

  test("map search=\"search index=dummy host=$host_var$\" maxsearches=20") {
    p(command(_), MapCommand(
      Pipeline(
        Seq(
          SearchCommand(
            Binary(
              Binary(
                Field("index"),
                Equals,
                Field("dummy")
              ),
              And,
              Binary(
                Field("host"),
                Equals,
                Variable("host_var")
              )
            )
          )
        )
      ),
      maxSearches = 20))
  }

  test(
    """map search="search index=dummy host=$host_var$ | eval this=\"that\" |
      |dedup 10 keepevents=true keepempty=false consecutive=true host ip port"""".stripMargin) {
    p(_map(_), MapCommand(
      Pipeline(
        Seq(
          SearchCommand(
            Binary(
              Binary(
                Field("index"),
                Equals,
                Field("dummy")
              ),
              And,
              Binary(
                Field("host"),
                Equals,
                Variable("host_var")
              )
            )
          ),
          EvalCommand(Seq(
            (Field("this"), StrValue("that"))
          )),
          DedupCommand(10,
            Seq(Field("host"), Field("ip"), Field("port")),
            keepEvents = true,
            keepEmpty = false,
            consecutive = true,
            SortCommand(Seq(
              (Some("+"), Field("_no"))
            ))
          )
        )
      ),
      maxSearches = 10))
  }

  // scalastyle:on
}
