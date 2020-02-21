package custormSource;/*
把collection集合作为数据源
使用并行度为1 的source
 */


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingDeamWithMyRichParalleSource {
    public static void main(String[] args) throws Exception {
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyRichParaleSource());
        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据:" + value);
                return value;
            }
        });

        //每2秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);
        String jobName = StreamingDeamWithMyRichParalleSource.class.getSimpleName();
        env.execute(jobName);
    }
}
