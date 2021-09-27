import org.apache.spark.sql.Transpiler

object Main extends App {
    println("Enter a Splunk command:")
    val splunkCommand: String = scala.io.StdIn.readLine()

    try {
        val pythonCode = Transpiler.toPython(splunkCommand)
        println(pythonCode)
    } catch {
        case err: Exception  =>
            println(err)
            System.exit(1)
    }
}
