package its.time




import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._
object torock {
  def main(args: Array[String]) {

    if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <path/to/edges>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("Load graph")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val edges: RDD[Edge[String]] =
      sc.textFile(args(0)).map { line =>
        val fields = line.split(",")
        Edge(fields(0).toLong, fields(1).toLong)
      }

    val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")

   
      val writer = new PrintWriter(new File("output.txt"))
      writer.write("num edges = " + graph.numEdges)
      writer.write("\n")
      writer.write("num vertices = " + graph.numVertices)
      writer.close()
   
  }
}