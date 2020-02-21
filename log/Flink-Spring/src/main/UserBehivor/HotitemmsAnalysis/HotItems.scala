package HotitemmsAnalysis

import org.apache.flink.api.common.state.StateTtlConfig.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment



// 输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
// 输出数据样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )
object HotItems {
  def main(args: Array[String]): Unit = {
    //创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //显示定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  }
}
