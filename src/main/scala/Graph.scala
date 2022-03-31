import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5
 
  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val graph: RDD[(Int,Int)]
         = sc.textFile(args(0))
             .map( ?? )               // create a graph edge (i,j), where i follows j

    var R: RDD[(Int,Int)]             // initial shortest distances
         = graph.groupByKey()
                .map( ?? )            // starting point has distance 0, while the others max_int

    for (i <- 0 until iterations) {
       R = R.join(graph)
            .flatMap( ?? )            // calculate distance alternatives
            .reduceByKey( ?? )        // for each node, find the shortest distance
    }

    R.filter( ?? )                    // keep only the vertices that can be reached
     .map( ?? )                       // prepare for the reduceByKey
     .reduceByKey( ?? )               // for each different distance, count the number of nodes that have this distance
     .sortByKey()
     .collect()
     .foreach(println)
  }
}
