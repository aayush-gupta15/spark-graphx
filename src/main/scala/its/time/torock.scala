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
 
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}   
 
val  newline = System.getProperty("line.separator")
    val conf = new SparkConf()
      .setAppName("Load graph")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)
 
    val sc = new SparkContext(conf)
 
    val edges: RDD[Edge[String]] =
      sc.textFile(args(0)).map { line =>
        val fields = line.split(",").map(elem => elem.trim)
        Edge(fields(0).toLong, fields(1).toLong)
      }
 
    val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
     
    val rank = graph.pageRank(0.001)
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)
     
    val writer = (new BufferedWriter(new OutputStreamWriter(new FileOutputStream("info.txt"))))  
   
     
       
      writer.write("No. edges = " + graph.numEdges + newline + "No. vertices = " + graph.numVertices )
      writer.write(newline + "Highest PageRank with value =" + rank.vertices.top(1)(Ordering.by(_._2)).mkString(newline))
      writer.write(newline +"NPI Id with Max InDegree  = " +  maxInDegree  + newline +"NPI Id with Max OutDegree = " + maxOutDegree + newline +"NPI Id with Highest Degree = "+ maxDegrees)
       
    
      
       
      writer.close()
    
  }
}
