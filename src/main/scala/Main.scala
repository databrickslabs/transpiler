import spl.Transpiler

object Main extends App {
  try {
    val splunkCommand: String = scala.io.Source.fromInputStream(System.in).mkString
    val pythonCode = Transpiler.toPython(splunkCommand)
    println(pythonCode)
  } catch {
    case err: Exception =>
      println(err.getMessage)
      System.exit(1)
  }
}
