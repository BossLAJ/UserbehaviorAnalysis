import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\data\\file"
    val outPath ="D:\\data\\result"

    //获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)
    //text.flatMap(_.toLowerCase.split("\\W+"))



  }
}
