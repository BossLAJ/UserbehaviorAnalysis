package transaction;

import custormSource.MyNooParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
union
合并多个流，多个source

 */
public class StreamingDeamUnion {
    public static void main(String[] args) throws Exception {
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text1 = env.addSource(new MyNooParalleSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNooParalleSource()).setParallelism(1);

        /*
        将两个数据源汇总为一个数据流，数据类型必须要相同
         */
        DataStream<Long> test = text1.union(text2);


        DataStream<Long> num = test.map(new MapFunction<Long, Long>() {
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据:" + value);
                return value;
            }
        });


        //每2秒钟处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);
        String jobName = StreamingDeamUnion.class.getSimpleName();
        env.execute(jobName);
    }
}
