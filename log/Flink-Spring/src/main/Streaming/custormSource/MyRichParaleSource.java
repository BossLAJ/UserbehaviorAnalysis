package custormSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/*
会额外多出open和close方法
针对source钟如果需要获取其他链接资源，那么可以在open方法钟获取资源链接，在close中关闭
 */
public class MyRichParaleSource extends RichParallelSourceFunction<Long> {
    private long count = 0;
    private boolean isRunning = true;
    /*
    主要运行的方法
    启动source数据源
    大部分情况下，都需要在run方法中实现一个循环，这样就可以循环产生数据
     */
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning){
            ctx.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }
    /*
    停止任务之前会调用cancel
    取消一个cancel的时候会调用的方法
    需要停止任务的时候，需要的操作（关闭一些链接）
     */
    public void cancel() {
        isRunning = false;
    }


    /*
    获取链接
    实现获取链接的代码
    这个方法只会在最开始的时候被调用一次
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open--------------");
        super.open(parameters);
    }

    /*
    关闭链接
    实现关闭链接的代码
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
