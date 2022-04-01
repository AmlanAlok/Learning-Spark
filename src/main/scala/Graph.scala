import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

//  ~/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --class Graph target/*.jar small-graph.csv
//      ~/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --class Graph target/cse6331-project5-0.1.jar small-graph.csv
//      spark-submit --class Graph target/cse6331-project5-0.1.jar small-graph.csv
object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5

  def lineToKeyValue(s: String): (Int, Int) = {
    val a = s.split(",")
    return (a(1).toInt, a(0).toInt)
  }

  def f2(k: Int, v: Iterable[Int]): (Int, (Int, Iterable[Int])) = {
    println("k="+k+", v="+v)

    if (k==1){
      return (k, (0, v))
    }
    else{
      return (k, (max_int, v))
    }
  }

  def f3(k:Int, x:  (Int, Iterable[Int])): Any = {
    println("in k ="+k)
    val following = x._2.toList
    if (x._1 < max_int){
//      x._2.foreach {
//        println
//        return (_, (x._1 + 1))
//      }
      println("following ="+following)

      for (i <- following){
        println("i="+i)
        return (i, (x._1 + 1))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleGraph")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))

    val kvPairs = lines.map(line => lineToKeyValue(line))
    kvPairs.collect().foreach(println)

    val followerList = kvPairs.groupByKey().map(x => f2(x._1,x._2)).sortByKey()       // sort can be removed. Just added for output clarity
    followerList.collect().foreach(println)

//    for (i <- 0 to iterations){
//      println("For i = "+i)
//    }
//
//    val join1 = followerList.join(kvPairs)
//    join1.collect().foreach(println)


    val p = followerList.map(x => f3(x._1, x._2))
    p.collect().foreach(println)

//
//    val j2 = kvPairs.join(followerList)
//    j2.collect().foreach(println)

//    val r = kvPairs.reduceByKey(_+_)
//    r.collect().foreach(println)
  }
 
//  def main ( args: Array[ String ] ) {
//    val conf = new SparkConf().setAppName("Graph")
//    val sc = new SparkContext(conf)
//
//    val graph: RDD[(Int,Int)]
//         = sc.textFile(args(0))
//             .map( ?? )               // create a graph edge (i,j), where i follows j
//
//    var R: RDD[(Int,Int)]             // initial shortest distances
//         = graph.groupByKey()
//                .map( ?? )            // starting point has distance 0, while the others max_int
//
//    for (i <- 0 until iterations) {
//       R = R.join(graph)
//            .flatMap( ?? )            // calculate distance alternatives
//            .reduceByKey( ?? )        // for each node, find the shortest distance
//    }
//
//    R.filter( ?? )                    // keep only the vertices that can be reached
//     .map( ?? )                       // prepare for the reduceByKey
//     .reduceByKey( ?? )               // for each different distance, count the number of nodes that have this distance
//     .sortByKey()
//     .collect()
//     .foreach(println)
//  }
}
