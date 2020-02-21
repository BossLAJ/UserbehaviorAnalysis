package Example_Month

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Sql")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val files = spark.sparkContext.textFile("D:\\IDEA2\\Example\\src\\access.log")

  }
}
