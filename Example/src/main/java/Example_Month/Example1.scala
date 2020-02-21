package Example_Month

import org.apache.spark.sql.SparkSession

object Example1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Example")
      .master("local[*]")
      .getOrCreate()

    val stus = spark.read.format("driver")
      .option("jdbc","com.jdbc.mysql.Driver")
      .option("url","mysql:jdbc://localhost:3306/bw_cms")
      .option("dbtable","t_scores")
      .option("user","root")
      .option("password","123456")
      .load()
     // .show()

    stus.createOrReplaceTempView("stu")
    //每个系数学成绩的前三名的学生统计结果中必须包含系、班和学生学号信息数学成绩
    spark.sql("select departmentid,classid,studentid,math,netil(5),over(partition by partmentid order by math desc )as ss from stus group by math").show()
  }
}
