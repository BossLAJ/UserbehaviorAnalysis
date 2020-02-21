package Example_Month

import org.apache.spark.sql.SparkSession

object Example6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //登陆表
    val logins = spark.sparkContext.textFile("D:\\IDEA2\\Example\\src\\login.txt")
      .map(line => line.split(" ")).map(x=>x.toList).toDF()

    //阅读表
    val pairs = spark.sparkContext.textFile("D:\\IDEA2\\Example\\src\\pair.txt")
      .map(line => line.split(" ")).map(x=>x.toList).toDF()

    //付费表
    val reads = spark.sparkContext.textFile("D:\\IDEA2\\Example\\src\\read.txt")
      .map(line=>line.split(" ")).map(x=>x.toList).toDF()

  }
}
