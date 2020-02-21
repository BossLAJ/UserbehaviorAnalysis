package WordCount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
    通过socket模拟产生单词数据，
    flink对数据进行单词统计，
    需要实现每隔1秒，对最近2秒内的数据进行统计汇总计算
 */
public class ScoketWordCount {
    public static void main(String[] args) throws Exception{
        //获取需要的端口
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.out.println("No port set use defalut port 9000");
            port = 9000;
        }
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname="hadoop101";
        String delimiter ="\n";
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        //a1   b1   c1
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }

        }).keyBy("word")
                //指定时间窗口大小为2秒，指定时间间隔为1秒
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");//使用sum和reduce都可以
/*.reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word,a.count+b.count);
                    }
                })*/
            //数据打印到控制台，并且设置并行度
            windowCount.print().setParallelism(1);
            //这一步代码一定要实现，否则程序不执行
            env.execute("Socket  window  count");

    }

    //自定义类型
    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(String word, long count){
            this.word=word;
            this.count=count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
