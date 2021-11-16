package org.apache.spark.sql

import java.io.{BufferedWriter, File, FileWriter}
import java.util.ArrayList

import org.apache.spark.api.python.{Py4JServer, PythonException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.util.Utils
import org.scalatest.Assertions

import scala.sys.process.{Process, ProcessLogger}


class Capture extends ProcessLogger {
  private val stdout = new ArrayList[String]()
  private val stderr = new ArrayList[String]()
  override def out(s: => String): Unit = stdout.add(s)
  override def err(s: => String): Unit = stderr.add(s)
  override def buffer[T](f: => T): T = f
  def getOutput: String = stdout.toArray.mkString("\n")
  def getError: String = stderr.toArray.mkString("\n")
}

trait ProcessProxy extends Logging {
  private val folder = getClass.getResource("/").getFile

  lazy val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.extensions", classOf[SplExtension].getName)
    .config("spark.sql.warehouse.dir", s"$folder/warehouse")
    .getOrCreate

  private val pyException = raw"(?m).*Exception: (.*)".r;

  def generates(search: String, expectedCode: String): Unit = {
    val generatedCode = Transpiler.toPython(search)
    readableAssert(expectedCode, generatedCode, "Code does not match")
  }

  def executes(search: String, results: String, truncate: Int = 0): Unit = {
    val pyOutput = launchPy(search, truncate)
    readableAssert(results, pyOutput, "Python results do not match")
    val df = Transpiler.toDataFrame(spark, search)
    val jvmOutput = df.showString(20, truncate)
    readableAssert(results, jvmOutput, "Results do not match")
  }

  private def launchPy(search: String, truncate: Int = 0): String = {
    val code = Transpiler.toPython(search)
    val file = wrapGeneratedCode(code, truncate)
    val gatewayServer = new Py4JServer(spark.sparkContext.conf)
    val thread = new Thread(() => Utils.logUncaughtExceptions { gatewayServer.start() })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()
    thread.join()
    try {
      val process = Process(s"python3 $file", new java.io.File(folder),
        "PYSPARK_GATEWAY_PORT" -> gatewayServer.getListeningPort.toString,
        "PYSPARK_GATEWAY_SECRET" -> gatewayServer.secret,
        "PATH" -> "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
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

  private def wrapGeneratedCode(code: String, truncate: Int = 0): File = {
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
         |df = $code
         |print(df._jdf.showString(20, $truncate, False))
         |""".stripMargin)
    writer.close()
    file
  }

  private def readableAssert(expected: String, actual: String, caption: String): Unit =
    if (actual != expected) {
      Assertions.fail(s"""FAILURE: $caption
        |${sideBySide(actual, expected).mkString("\n")}
        |""".stripMargin)
    }
}
