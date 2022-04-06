import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5

  def lineToKeyValue(s: String): (Int, Int) = {
    val a = s.split(",")
    return (a(1).toInt, a(0).toInt)
  }

  def getDistance(k: Int): (Int, Int) = {

    if (k==1 || k == start_id)
      return (k, 0)
    else
      return (k, max_int)
  }

  def assignNewDistance(x: (Int, (Int, Iterable[(Int)]))): List[(Int, (Int, Iterable[(Int)]))] = {

    val k = x._1
    val value = x._2
    val d = value._1
    val nodes = value._2
    var newList = List.empty[(Int, (Int, Iterable[Int]))]
    val emptyList = List.empty[(Int)]

    newList :+= x

    if (d < max_int) {
      for (i<-nodes){
        newList :+= (i, (d+1, emptyList))
      }
    }

    //    println("newList ="+newList)
    return newList
  }

  def chooseMinDistance(x: (Int, Iterable[Int]), y:  (Int, Iterable[Int])):  (Int, Iterable[Int])={

    var d = 0

    if (x._1 < y._1)
      d = x._1
    else
      d = y._1

    if (x._2.isEmpty)
      return (d, y._2)
    else
      return (d, x._2)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))

    val kvFollowerList = lines.map(line => lineToKeyValue(line))
      .groupByKey()
    //    println("kvFollowerList")
    //    kvFollowerList.collect().foreach(println)

    val kvDistance = kvFollowerList.map(x => getDistance(x._1))
    //    println("kvDistance")
    //    kvDistance.collect().foreach(println)

    var join = kvDistance.join(kvFollowerList)
    //    println("join")
    //    join.collect().foreach(println)

    for (i <- 0 until iterations){
//      println("Interation ="+i)
      join = join.flatMap(x => assignNewDistance(x))
        .reduceByKey((x,y) => chooseMinDistance(x,y))
      //      join.collect().foreach(println)
    }

    val filterOutput = join.filter(x => x._2._1 < max_int)
      .map(x => (x._2._1, 1))
      .reduceByKey(_+_)
      .sortByKey()
    //                            .collect().foreach(println)

    println("Printing output")
    filterOutput.collect().foreach(println)
    println("Printing output completed ------------")
  }
}
