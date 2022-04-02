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

//  def f2(k: Int, v: Iterable[Int]): (Int, (Int, Iterable[Int])) = {
  def f2(k: Int, v: Iterable[Int]): (Int, Int) = {
//    println("k="+k+", v="+v)
    if (k==1){
//      return (k, (0, v))
      return (k, 0)
    }
    else{
//      return (k, (max_int, v))
      return (k, max_int)
    }
  }

  def f3(k:Int, x:  (Int, Iterable[Int])): Any = {
//    println("in k ="+k)
    val following = x._2.toList
    if (x._1 < max_int){
//      x._2.foreach {
//        println
//        return (_, (x._1 + 1))
//      }
//      println("following ="+following)

      for (i <- following){
//        println("i="+i)
        return (i, (x._1 + 1))
      }
    }
  }

//  f4(distance: Int, node: Int) = {
//    println("d ="+distance+", node ="+node)
//    return List(node, distance)
//  }

  def f4(x: (Int, (Int, Int))): List[(Int, Int)] = {
//    println("d ="+distance+", node ="+node)
//    return List(node, distance)
    println("Inside f4")
    println("k =", x._1)
    println(x)
    return List(x._2)
  }

  def f5(x: (Int, Iterable[(Int, Int)])): List[(Int, (Int, Int))] = {

    val nums = x._2.toList
    var newList = List.empty[(Int, (Int, Int))]
    val k =x._1

    for (i<-nums){

      val d = i._1
      val node = i._2

      println("d="+d+", node = "+node)

      if (d<max_int){
//        if (d==0){
//          newList :+= (x._1, d)
//        }
        newList :+= (k, (node, d+1))
      }
      else{
        newList :+= (k, (node, d))
      }
    }
    println("newList ="+newList)
    return newList
  }

  def f6(x: (Int, (Int, Iterable[(Int)]))): List[(Int, (Int, Iterable[(Int)]))] = {

    val k = x._1
    val value = x._2
    val d = value._1
    val nodes = value._2
    var newList = List.empty[(Int, (Int, Iterable[Int]))]
//    val emptyList = List.empty[()]
    val emptyList = List.empty[(Int)]

    newList :+= x

    if (d < max_int) {
      for (i<-nodes){
        newList :+= (i, (d+1, emptyList))
      }
    }

    println("newList ="+newList)
    return newList
  }

  def f7(x: (Int, Iterable[Int]), y:  (Int, Iterable[Int])):  (Int, Iterable[Int])={

    var d = 0
//    var p: Iterable[Int]

    if (x._1 < y._1) {
      d = x._1
    }
    else{
      d = y._1
    }

    if (x._2.isEmpty){
//      p = y._2
      return (d, y._2)
    }
    else{
//      p = x._2
      return (d, x._2)
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))

    val kvPairs = lines.map(line => lineToKeyValue(line)).groupByKey()
    println("kvPairs")
    kvPairs.collect().foreach(println)

//    kvPairs.flatMap(x => (x._1, x._2)).collect().foreach(println)

    val followerList = kvPairs.map(x => f2(x._1,x._2)).sortByKey()       // sort can be removed. Just added for output clarity
    println("followerList")
    followerList.collect().foreach(println)

    var join1 = followerList.join(kvPairs)
    println("join1")
    join1.collect().foreach(println)

    for (i <- 0 to iterations){
      println("For i = "+i)

      join1 = join1.flatMap(x => f6(x)).reduceByKey((x,y) => f7(x,y))
      println("updateDist")
      join1.collect().foreach(println)

//      val updateDist = join1.flatMap(x => f6(x))
//      println("updateDist")
//      updateDist.collect().foreach(println)
//
//      val red = updateDist.reduceByKey((x,y) => f7(x,y))
//      println("reduce")
//      red.collect().foreach(println)
    }

//    val rdk = join1.reduceByKey(_+_)
//    rdk.collect().foreach(println)



    //    for (i <- 0 to iterations){
    //      println("For i = "+i)
    //    }
    //
//    val p = followerList.map(x => f3(x._1, x._2))
//    p.collect().foreach(println)

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
