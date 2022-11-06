package org.apache.spark.sql

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.api.python.{Py4JServer, PythonException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.util.Utils
import org.scalatest.Assertions
import spl.Transpiler

import scala.sys.process.{Process, ProcessLogger}
import scala.util.matching.Regex


class Capture extends ProcessLogger {
  private val stdout = new java.util.ArrayList[String]()
  private val stderr = new java.util.ArrayList[String]()
  override def out(s: => String): Unit = stdout.add(s)
  override def err(s: => String): Unit = stderr.add(s)
  override def buffer[T](f: => T): T = f
  def getOutput: String = stdout.toArray.mkString("\n")
  def getError: String = stderr.toArray.mkString("\n")
}

trait ProcessProxy extends Logging {
  private val folder = getClass.getResource("/").getFile

  protected lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.extensions", classOf[SplExtension].getName)
    .config("spark.sql.warehouse.dir", s"$folder/warehouse")
    .getOrCreate

  private val pyException = raw"(?m).*Exception: (.*)".r

  def generates(search: String, expectedCode: String): Unit = {
    val generatedCode = Transpiler.toPython(search)
    readableAssert(expectedCode, generatedCode,
      "Expected (left) and generated (right) do not match")
  }

  def extractExceptionIfExists(search: String): String = {
    val pattern: Regex = "# Error in ([a-zA-Z]+):(.*)".r
    val generatedCode = Transpiler.toPython(search)

    pattern.findFirstMatchIn(generatedCode) match {
      case Some(value) => generatedCode
      case None => ""
    }
  }

  def executes(search: String, results: String, truncate: Int = 0): Unit = {
    val pyOutput = launchPy(search, truncate)
    readableAssert(results, pyOutput, "Expected (left) and Python (right) results do not match")
    val df = Transpiler.toDataFrame(spark, search)
    val jvmOutput = df.showString(20, truncate, vertical = false)
    readableAssert(pyOutput, jvmOutput, "Python (left) and JVM (right) results do not match")
  }

  private def launchPy(search: String, truncate: Int): String = {
    val code = Transpiler.toPython(search)
    val file = wrapGeneratedCode(code, truncate)
    val gatewayServer = new Py4JServer(spark.sparkContext.conf)
    val thread = new Thread(() => Utils.logUncaughtExceptions { gatewayServer.start() })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()
    thread.join()
    try {
      val env = Map(
        "PYSPARK_GATEWAY_PORT" -> gatewayServer.getListeningPort.toString,
        "PYSPARK_GATEWAY_SECRET" -> gatewayServer.secret,
        "PATH" -> "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
      val process = Process(s"python3 $file", new java.io.File(folder), env.toSeq: _*)
      val capture = new Capture
      if (process.!(capture) != 0) {
        val error = capture.getError
        val message = pyException.findFirstIn(error) match {
          case Some(value) => value
          case None => error
        }
        throw new PythonException(s"$message\n\nGENERATED = $code", null)
      }
      capture.getOutput
    } finally {
      gatewayServer.shutdown()
      file.delete()
    }
  }

  private def wrapGeneratedCode(code: String, truncate: Int): File = {
    val file = File.createTempFile("spl", ".py", new File(folder))
    file.setExecutable(true)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(
      s"""#!env python3
         |from pyspark.context import SparkContext
         |from pyspark.sql import SparkSession
         |from pyspark.conf import SparkConf
         |
         |SparkContext._ensure_initialized()
         |_sc = SparkContext._jvm.SparkSession.getDefaultSession().get().sparkContext()
         |SparkContext(conf=SparkConf(_jconf=_sc.conf()),
         |             jsc=SparkContext._jvm.JavaSparkContext(_sc))
         |spark = SparkSession.builder.getOrCreate()
         |
         |import pyspark.sql.functions as F
         |from pyspark.sql.window import Window
         |
         |df = $code
         |print(df._jdf.showString(20, $truncate, False))
         |""".stripMargin)
    writer.close()
    file
  }

  def readableAssert(expected: String, actual: String, caption: String): Unit =
    if (actual != expected) {
      Assertions.fail(s"""FAILURE: $caption
        |${sideBySide(expected, actual).mkString("\n")}
        |""".stripMargin)
    }
}
