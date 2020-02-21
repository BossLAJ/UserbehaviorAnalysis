package WordCount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*
  滑动窗口
  每隔1秒进行最近两秒内的数据，统计打印到控制台
 */

object SocketWordCountScala {
//  def main(args: Array[String]): Unit = {
//    //获取socket端口号
//    val port:Int = try {
//      //如果解析成功
//      ParameterTool.fromArgs(args).getInt("port")
//    }catch {
//      case e:Exception =>{
//        System.err.println("No port set use defalut 9000")
//      }9000
//    }


//    //1. 获取运行环境
//    val env:StreamExecutionEnvironment  =StreamExecutionEnvironment.getExecutionEnvironment
//
//    //2.连接socket获取输入数据
//    val text = env.socketTextStream("hadoop101",port,'\n')
//
//    import org.apache.flink.api.scala._
//    //解析数据（将数据打平），分组，窗口计算，并且聚合求sum
//    val windowCounts = text.flatMap(line => line.split("\\s"))
//      .map(w => WordWithCount(w,1))
//      .keyBy("word")
//      .timeWindow(Time.seconds(2),Time.seconds(1))
//      .sum("count")
//
//    windowCounts.print().setParallelism(1);//打印到控制台，单线程去跑
//    env.execute("socket window count")
//
//  }
//
//  //自定义一个类
//  case class WordWithCount(word:String,count:Long)
}
