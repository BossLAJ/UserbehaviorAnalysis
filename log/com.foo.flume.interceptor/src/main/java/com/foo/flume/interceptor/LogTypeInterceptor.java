package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {


    @Override
    public void initialize() {
        //初始化方法
    }

    @Override
    public Event intercept(Event event) {
        //获取flume的数组body，body为json数组
        byte[] body = event.getBody();
        //将json数组转化为字符串
        String jsonStr = new String(body);
        String logType = " ";
        //判断日志类型。start为启动日志，event为事件日志
        if (jsonStr.contains("start")){
            logType = "start";
        }else{
            logType = "event";
        }

        //获取flume的消息头
        Map<String, String> headers = event.getHeaders();
        //将日志类型添加到flume的消息头
        headers.put("logType",logType);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        //关闭方法
    }

    public static class Bulider implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
