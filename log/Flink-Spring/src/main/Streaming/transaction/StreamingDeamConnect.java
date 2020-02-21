package transaction;

import custormSource.MyNooParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/*
connect
只能连接两个流，数据类型可以相同也可以不同，对不同的数据源使用不同的处理方式
 */
public class StreamingDeamConnect {
    public static void main(String[] args) throws Exception {
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取两个数据源
        DataStreamSource<Long> text1 = env.addSource(new MyNooParalleSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNooParalleSource()).setParallelism(1);
        //将第二个数据流转为String类型
        SingleOutputStreamOperator<String> test2_str = text2.map(new MapFunction<Long, String>() {
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        ConnectedStreams<Long, String> connectStream = text1.connect(test2_str);

        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>() {

            //处理不同的数据
            public Object map1(Long value) throws Exception {
                return value;
            }

            //处理不同的数据
            public Object map2(String str) throws Exception {
                return str;
            }
        });

        //打印结果
        result.print().setParallelism(1);
        String jobName = StreamingDeamConnect.class.getSimpleName();
        env.execute(jobName);
    }
}
