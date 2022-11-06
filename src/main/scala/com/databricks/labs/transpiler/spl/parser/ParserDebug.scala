package com.databricks.labs.transpiler.spl.parser

import fastparse._

private[parser] class ParserDebug[R](r: R) {
  def @@ (implicit ctx: P[_], name: sourcecode.Name): R = {
    if (ctx.logDepth == -1) return r
    val indent = "  " * ctx.logDepth
    val rep = ctx.successValue.toString.replaceAll("ArrayBuffer", "Seq")
    val debug = rep.substring(0, Math.min(rep.length, 128))
    print(s"$indent@${name.value}: $debug\n")
    r
  }

  def tap(l: R => String): R = {
    // scalastyle:off
    println(l(r))
    // scalastyle:on
    r
  }
}
