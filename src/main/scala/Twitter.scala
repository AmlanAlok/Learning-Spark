import org.apache.spark._
import org.apache.spark.sql._

object Twitter {

  case class Follows ( user: Int, follower: Int )

  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Twitter")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /* ... */

    val inputData = spark.read.csv(args(0))
    inputData.show()


  }
}
