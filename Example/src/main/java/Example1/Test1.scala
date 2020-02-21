package Example1

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object Test1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().appName("Spark sql")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data= spark.sparkContext.textFile("D:\\IDEA2\\Example\\access.log")

    //8.35.201.166 - - [30/May/2013:17:38:21 +0800] "GET /static/image/common/nv.png HTTP/1.1" 200 1939
    val unit = data.map(x=>Row(
      x.split(" ")(0),
      formatDate(x.split("\\[")(1).split("]")(0)),
      x.split("\"")(1).split("\"")(0),
      x.split(" ")(x.split(" ").length-2),
      x.split(" ")(x.split(" ").length-1)
    ))

    val a = Array("ip","time","url","status","size")
    val h = a.map(x=>StructField(x,StringType))
    val structType = StructType(h)
    val frame = spark.createDataFrame(unit,structType)
    //统计一分钟内访问站次数最多的前5个客户端，显示客户端名称和访问次数
    frame.groupBy(substring($"time",0,12)).count().orderBy(col("count").desc).show(5)
    //统计从30/May/2013:17:00:00开始，至30/May/2013:23:59:59止，每小时每个客户端的访问次数
    val pos = formatDate("30/May/2013:17:00:00")
    val len = formatDate("30/May/2013:23:59:59")
//    frame.where($"time".between(pos,len)).groupBy($"ip",$"time".substr(0,10)).count().show())

    frame.where("url like '%swfupload.swf%'").select(count("*")).show()

  }

  def formatDate(date: String): String ={
    val format1 = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss",Locale.ENGLISH)
    val date1 = format1.parse(date)
    val format2 = new SimpleDateFormat("yyyyMMddhhmmss",Locale.ENGLISH)
    format2.format(date1)
  }
}


