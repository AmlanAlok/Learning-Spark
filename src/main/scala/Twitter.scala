import org.apache.spark._
import org.apache.spark.sql._

object Twitter {

  case class Follows ( user: Int, follower: Int )

  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Twitter")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /* Reading data from data source */
//    val inputData = spark.read.csv(args(0))
////      .as[Follows]

    val in = spark.sparkContext.textFile(args(0))
      .map(_.split(",")).map(x => Follows(x(0).toInt, x(1).toInt)).toDF()

    in.show()
    in.printSchema()




//    dataframe.write.csv("path")

  }
}
