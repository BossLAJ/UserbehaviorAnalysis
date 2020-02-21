import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//隐私错误需要导入的包
import org.apache.flink.streaming.api.scala._
//引入
import  scala.collection.JavaConversions._


//输入数据样例类
case class UserBehavior(userid :Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long )
//输出样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //创建一个env环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //显示的定义time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //定义一个流
    val stream = env
      .readTextFile("D:\\IDEA2\\UserbehaviorAnalysis\\HotltemsAnalysis\\src\\main\\resources\\UserBehaviorTest.csv")
      .map(line => {
        val lineArray = line.split(",")
        UserBehavior(lineArray(0).toLong, lineArray(1).toLong, lineArray(2).toInt, lineArray(3), lineArray(4).toLong)
      })
      //指定时间戳和watermark  转化为毫秒
      .assignAscendingTimestamps(_.timestamp * 1000)
        .filter(_.behavior == "pv")
        .keyBy("itemId")//分流
      //时间窗口的时间是1，滑动距离是5分钟
        .timeWindow(Time.hours(1),Time.minutes(5))
      //预先操作，来一个数据，累加器+1，
        .aggregate(new CountAgg(),new WindowResultFunction() )
        .keyBy("windowEnd")
      //TopNHotItems当成本身类的属性
        .process(new TopNHotItems(3))
        .print()

    //调用execute去执行任务
    env.execute("Hot Items Job")
  }

  //自定义实现聚合函数
  class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc+1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+acc1
  }

  //自定义实现windowFunction  输出ItemViewCount格式
  class WindowResultFunction()extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      //获取itemid
      val itemId:Long = key.asInstanceOf[Tuple1[Long]].f0
      //获取count，因为input是iterator类型，用.next获取
      val count = input.iterator.next()

      out.collect(ItemViewCount(itemId,window.getEnd,count))
    }
  }

  //自定义实现process Function
  class TopNHotItems(topSize: Int)extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

    //定义状态ListState
    private var itemState:ListState[ItemViewCount] = _
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //给状态变量命名和定义和类型
      //描述器
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }
    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      //注册定时器，触发事件为  windEnd + 1,触发时说明widow已经收集完成所有数据
      context.timerService.registerEventTimeTimer(i.windowEnd+1)
    }

      //定时器触发操作    从state里取出所有数据排序topN，输出
      // 定时器触发操作，从state里取出所有数据，排序取TopN，输出
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 获取所有的商品点击信息
        val allItems: ListBuffer[ItemViewCount] = ListBuffer()
        for(item <- itemState.get){
          allItems += item
        }
        // 清楚状态中的数据，释放空间
        itemState.clear()

        //按照点击量从大到小排序,逆序排序，选取topN
        val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
        //将排名数据格式化，便于打印输出
        val result :StringBuilder = new StringBuilder
        result.append("=========================================\n")
        //-1是因为初始+1
        result.append("时间：").append(new Timestamp(timestamp -1)).append("\n")
        for(i <- sortedItems.indices){
          val currentItem :ItemViewCount = sortedItems(i)
          // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
          result.append("No").append(i+1).append(":")
            .append("   商品ID=").append(currentItem.itemId)
            .append("   浏览量=").append(currentItem.count).append("\n")
        }
        result.append("========================================\n\n")
        //控制台输出频率
        Thread.sleep(100)
        out.collect(result.toString())
    }
  }
}
