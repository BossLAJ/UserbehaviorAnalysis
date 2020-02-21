/*
把collection集合作为数据源
 */


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StreamingFromCollection {
    public static void main(String[] args) throws Exception {
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Integer> data = new ArrayList<Integer>();
        data.add(10);
        data.add(20);
        data.add(30);

        //指定数据源
        DataStreamSource<Integer> CollectionData = env.fromCollection(data);

        //通过map对数据源进行处理
        DataStream<Integer> num = CollectionData.map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer value) throws Exception {
                //数据处理
                return value + 1;
            }
        });
        //直接打印
        num.print().setParallelism(1);
        env.execute("StreamingFromCollection");
    }
}
