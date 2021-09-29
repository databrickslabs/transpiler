package org.apache.spark.sql

import scala.collection.mutable
import scala.sys.process.{Process, ProcessLogger}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.catalyst.util.sideBySide
import org.scalatest.Assertions


class Capture extends ProcessLogger {
  private val stdout = new mutable.StringBuilder()
  private val stderr = new mutable.StringBuilder()
  override def out(s: => String): Unit = stdout ++= s
  override def err(s: => String): Unit = stderr ++= s
  override def buffer[T](f: => T): T = f
  def getOutput: String = stdout.toString
  def getError: String = stderr.toString
}

trait ProcessProxy extends Logging {
  private val folder = getClass.getResource("/").getFile

  lazy val spark = {
    SparkSession.builder()
      .withExtensions(e => new SplExtension().apply(e))
      .master("local[1]")
      .config("spark.sql.warehouse.dir", s"$folder/warehouse")
      .getOrCreate()
  }

  def launchPy = {
    val pb = Process(s"$folder/t.py",
      new java.io.File(folder),
      "SPARK_MASTER" -> s"spark://localhost:${spark.conf.get("spark.driver.port")}",
      "PATH" -> "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
    val pl = new Capture
    pb.!(pl)

    print(pl.getOutput)
  }

  def generates(search: String, expectedCode: String) = {
    val generatedCode = Transpiler.toPython(search)
    if (generatedCode != expectedCode) {
      Assertions.fail(s"""FAILURE: Code does not match
              |=======
              |${sideBySide(generatedCode, expectedCode).mkString("\n")}
              |""".stripMargin)
    }
  }

  def executes(search: String, results: String, truncate: Int = 0) = {
    val testVar = Transpiler.toDataFrame(spark, search)
                            .showString(20, truncate)
    if (testVar != results) {
      Assertions.fail(s"""FAILURE: Results do not match
                         |=======
                         |${sideBySide(testVar, results).mkString("\n")}
                         |""".stripMargin)
    }
  }
}
