package Example_Month

import org.apache.spark.sql.SparkSession

object Example5 {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val files = spark.sparkContext.textFile("D:\\IDEA2\\Example\\src\\order.log")
      .map(line=>line.split(" ")).map(x=>x.toList).toDF().createOrReplaceTempView("file")



  }
}
