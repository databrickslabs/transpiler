package spl

class SplParserTest extends ParserSuite {
  import spl.SplParser._

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
    p(fieldAndValueList(_), Map(
      "foo" -> "bar",
      "bar" -> "baz"
    ))
  }

  test("a ,   b,c, d") {
    p(fieldList(_), Seq(
      Field("a"),
      Field("b"),
      Field("c"),
      Field("d"),
    ))
  }

  test("D:\\Work\\Stuff.xls") {
    p(filename(_), "D:\\Work\\Stuff.xls")
  }

  test("-100500") {
    p(int(_), IntValue(-100500))
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
        Field("str_3"),
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
        Call("TERM",List(Field("XXXXX*\\\\XXXXX*")))
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
          (None, Field("A")),
        )
      )
    )
  }

  test("eval mitre_category=\"Discovery\"") {
    p(eval(_), EvalCommand(Seq(
      (Field("mitre_category"), StrValue("Discovery"))
    )))
  }

  test("eval hash_sha256= lower(hash_sha256), b=c") {
    p(eval(_), EvalCommand(Seq(
      (Field("hash_sha256"), Call("lower", Seq(Field("hash_sha256")))),
      (Field("b"), Field("c"))
    )))
  }

  test("convert ctime(indextime)") {
    p(convert(_), ConvertCommand(None, Seq(
      FieldConversion("ctime", Field("indextime"), None)
    )))
  }

  test("collect index=threathunting a=b x, y,  z") {
    p(pipeline(_), Pipeline(Seq(
      CollectCommand(Map(
        "index" -> "threathunting",
        "a" -> "b"
      ), Seq(
        Field("x"),
        Field("y"),
        Field("z"),
      ))
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
      CollectCommand(Map(
        "index" -> "newer"
      ),Seq())
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
        Field("baz*"),
      ))
    )))
  }

  test("stats first(startTime) AS startTime, last(histID) AS lastPassHistId BY testCaseId") {
    p(pipeline(_), Pipeline(Seq(
      StatsCommand(Map(),Seq(
        Alias(
          Call("first",Seq(
            Field("startTime")
          )),
          "startTime"),
        Alias(
          Call("last",Seq(
            Field("histID")
          )),
          "lastPassHistId")
      ),
      Seq(
        Field("testCaseId")
      ))
    )))
  }

  test("stats count(eval(status=404))") {
    p(pipeline(_), Pipeline(Seq(
      StatsCommand(Map(),Seq(
        Call("count",Seq(
          Call("eval",Seq(
            Binary(
              Field("status"),
              Equals,
              IntValue(404)
            ))))))
      ))
    ))
  }

  test("no-comma stats") {
    val query =
      """stats allnum=f delim=":" partitions=10 count earliest(_time) as earliest latest(_time) as latest
        |values(var_2) as var_2
        |by var_1
        |""".stripMargin
    parses(query, stats(_), StatsCommand(
      Map(
        "allnum" -> "f",
        "delim" -> ":",
        "partitions" -> "10"
      ),
      Seq(
        Call("count"),
        Alias(Call("earliest", Seq(Field("_time"))), "earliest"),
        Alias(Call("latest", Seq(Field("_time"))), "latest"),
        Alias(Call("values", Seq(Field("var_2"))), "var_2"),
      ),
      Seq(
        Field("var_1")
      )
    ))
  }

  test("rex field=savedsearch_id max_match=10 \"(?<user>\\w+);(?<app>\\w+);(?<SavedSearchName>\\w+)\"") {
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

  test("join type=left usetime=true earlier=false overwrite=false product_id, host, name [search vendors]") {
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
        Alias(Field("port"), "host"),
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
        spl.Field("host"),
        spl.Field("ip"),
        spl.Field("port")
      ),
      keepEvents = true,
      keepEmpty = false,
      consecutive = true,
      SortCommand(Seq((Some("+"), spl.Field("_no"))))
    ))
  }

  test("dedup 10 keepevents=true host ip port sortby +host -ip") {
    p(dedup(_), DedupCommand(
      10,
      Seq(
        spl.Field("host"),
        spl.Field("ip"),
        spl.Field("port")
      ),
      keepEvents = true,
      keepEmpty = false,
      consecutive = false,
      SortCommand(
        Seq(
          (Some("+"), Field("host")),
          (Some("-"), Field("ip")),
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
      args = FormatArgs("(", "(", "AND", ")", "OR", ")")
    ))
  }

  test("format mvsep=\"||\" \"[\" \"[\" \"&&\" \"]\" \"||\" \"]\"") {
    p(format(_), FormatCommand(
      mvSep = "||",
      maxResults = 0,
      args = FormatArgs("[", "[", "&&", "]", "||", "]")
    ))
  }
}
