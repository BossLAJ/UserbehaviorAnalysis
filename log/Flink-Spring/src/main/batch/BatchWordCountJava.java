import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
        String inpath = "D:\\data\\file";
        String outPath = "D:\\data\\result";
        //获取数据源
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获取文件中的内容
        DataSource<String> text = env.readTextFile(inpath);

        //对文件中的内容进行处理
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(outPath,"\n"," ");
        env.execute("batch word count");
    }

    //创建新的类,具体处理文件
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{
        //抽象方法
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toString().split("\\W+");
            for(String token :tokens){
                if (token.length()>0){
                    out.collect(new Tuple2<String, Integer>(token,1));
                }
            }
        }
    }
}
