package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        //获取传输过来的数据
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        //此处body为原始数据，需要处理
        if (LogUtils.validateReportLog(body)){
            //通过了校验之后得到的数据就是我们的目标数据
            return event;
        }
        //没有通过校验，不是我们需要的数据
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = new ArrayList<>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null){
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    @Override
    public void close() {

    }
    public static class Bulider implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
