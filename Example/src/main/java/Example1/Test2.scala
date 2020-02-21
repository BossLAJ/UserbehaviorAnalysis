package Example1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Month")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //编写spark程序。读取数据

    val stus = spark
      .read
      .format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/bw_cms")
      .option("dbtable","t_scores")
      .option("user","root")
      .option("password","123456")
      .load()

    stus.createOrReplaceTempView("stu")
    //
    //    //每个班所登记的各课程成绩总数，在统计结果中必须包含系、班，以及各课程成绩
    //    spark.sql("select departmentid,classid,sum(language),sum(math),sum(english) from stu group by departmentid,classid").show()
    //    stus.groupBy("departmentid","classid").sum("language","math","english").show()
    //
    ////    //分别用DataFrame API和Spark SQL统计每个系，每个班各课程的总成绩和平均成绩（保留两位小数），在统计结果中必须包含系、班的信息，以及各课程成绩
    ////    spark.sql("select departmentid,classid,round(sum(language),2)as sumLanguage, round(sum(math),2)as sumMath,round(sum(english),2)as sumEngilsh,"
    ////    +"round(avg(language),2)as avgLanguage,round(avg(math),2)as avgMath,round(avg(english),2) as avgEnglish from stu group by departmentid,classid"
    ////    ).show()
    ////
    ////    stus.select("departmentid","classid","studentid","math","language","english")
    ////      .groupBy("departmentid","classid")
    ////      .agg(sum("language")as("sumLanguage"),sum("math")as("sumMath"),sum("english")as("sumEnglish"),
    ////        round(avg("language"),2)as("avgLanguage"),
    ////        round(avg("math"),2)as("avgMath"),
    ////        round(avg("english"),2)as("english")
    ////      ).show()

////    （5）分别用DataFrame API和Spark SQL统计每个系数学成绩的前三名的学生，在统计结果中必须包含系、班和学生学号信息，以及数学成绩，要注意对相同成绩的处理
 spark.sql("select a.departmentid,a.classid,a.studentid,a.math,a.num from (se" +
   "lect departmentid,classid,studentid,math,dense_rank() over(partition by departmentid order by math desc)as num from stu)a where a.num <=3").show()
//  val overs = Window.partitionBy("departmentid").orderBy(col("math").desc)
//  stus.select("departmentid","classid","studentid","math").withColumn("top",dense_rank()over(overs)).where($"top"<=3).show()

  //    //（6）分别用DataFrame API和Spark SQL统计每个系三课程总成绩排名前三的学生，在统计结果中必须包含系、班和学生学号信息，以及各课程成绩和三课程总成绩信息
   spark.sql("select x.departmentid,x.classid,x.studentid,x.language,x.math,x.english,x.sum,x.num from(select a.departmentid,a.classid,a.studentid,a.language,a.math,a.english,a.sum,row_number() over(partition by departmentid order by a.sum desc ) as num from (select departmentid,classid,studentid,language,math,english,(language+math+english) as sum from stu) a) x where x.num<=3").show()

   //val row = Window.partitionBy("departmentid").orderBy($"sumAll".desc)stus.select("departmentid","classid","studentid","language","math","english").withColumn("sumAll",$"language"+$"math"+$"english").withColumn("top",row_number() over(row)).where($"top"<=3).show()
  }
}
