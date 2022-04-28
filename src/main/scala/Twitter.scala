import org.apache.spark._
import org.apache.spark.sql._

object Twitter {

  case class Follows ( user: Int, follower: Int )

  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Twitter")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val inputDF = spark.sparkContext.textFile(args(0))
      .map(_.split(",")).map(x => Follows(x(0).toInt, x(1).toInt)).toDF()

//    println("-----------"+inputDF.getClass)
    inputDF.show()
    inputDF.printSchema()

    inputDF.createOrReplaceTempView("X")

    val output1 = spark.sql("" +
      "SELECT x.follower, count(x.user) no_of_users_followed " +
      "FROM X x " +
      "GROUP BY x.follower")
    println("-----------"+output1.getClass)
//    output1.show()

    output1.createOrReplaceTempView("Y")

    val output2 = spark.sql("" +
      "SELECT y.no_of_users_followed, count(y.follower) user_count " +
      "FROM Y y " +
      "GROUP BY y.no_of_users_followed " +
      "ORDER BY y.no_of_users_followed")

//    output2.show()

    output2.collect().foreach(println)

  }
}
