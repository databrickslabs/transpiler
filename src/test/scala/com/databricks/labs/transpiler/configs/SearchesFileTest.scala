package com.databricks.labs.transpiler.configs

import org.apache.spark.internal.Logging
import org.scalatest.funsuite.AnyFunSuite

class SearchesFileTest extends AnyFunSuite with Logging {

  private def res = getClass.getResourceAsStream(_)

  test("att&ck") {
    new SearchesFile(res("/savedsearches.conf"))
  }

  test("expansion") {
    val sc = ConfigContext(
      new SearchesFile(res("/savedsearches.conf")),
      new MacrosFile(res("/macros.conf")))

    val plan = sc.generatePython("[T1101] Security Support Provider")
    log.info(s"Generated code: \n$plan")
  }
}
