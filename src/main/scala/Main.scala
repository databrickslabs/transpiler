import spl.Transpiler

object Main extends App {
  try {
    val splunkCommand: String = scala.io.Source.fromInputStream(System.in).mkString
    val pythonCode = Transpiler.toPython(splunkCommand)
    // scalastyle:off println
    println(pythonCode)
    // scalastyle:on println
  } catch {
    case err: Exception =>
      // scalastyle:off println
      println(err.getMessage)
      // scalastyle:on println
      System.exit(1)
  }
}
