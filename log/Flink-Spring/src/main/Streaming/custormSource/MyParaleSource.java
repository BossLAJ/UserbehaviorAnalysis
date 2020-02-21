package custormSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/*
自定义实现一个支持并行度的source
 */
public class MyParaleSource implements ParallelSourceFunction<Long> {
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
}
