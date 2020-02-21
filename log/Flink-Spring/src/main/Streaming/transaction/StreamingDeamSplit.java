package transaction;

import custormSource.MyNooParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

/*
split
应用场景：
根据规则将一个数据流切分为多个数据流
可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，
所以就可以根据一定的规则，
把一个数据流，切分为多个数据流，
这样每个数据流就可以使用不同的处理逻辑

 */
public class StreamingDeamSplit {
    public static void main(String[] args) throws Exception {
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNooParalleSource()).setParallelism(1);

        //切分数据流，按照数据的奇偶性进行分区
        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            public Iterable<String> select(Long value) {
                //标识
                ArrayList<String> outPut = new ArrayList<String>();
                if (value % 2 == 0) {
                    outPut.add("even");//偶数
                } else {
                    outPut.add("odd");//奇数
                }
                return outPut;
            }
        });

        //选择一个或者多个切分后的流
        //偶数流
        DataStream<Long> evenStream = splitStream.select("even");
        //奇数流
        DataStream<Long> oddStream = splitStream.select("odd");

        //打印结果
        evenStream.print().setParallelism(1);
        oddStream.print().setParallelism(1);

        String jobName = StreamingDeamSplit.class.getSimpleName();
        env.execute(jobName);
    }
}
