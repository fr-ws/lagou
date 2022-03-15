package cn.lagou.dw.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.compress.utils.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HomeworkInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override

//逐条处理event
//    自定义拦截器的实现：
//1、获取 event 的 header
//2、获取 event 的 body
//3、解析body获取json串
//4、解析json串获取时间戳
//5、将时间戳转换为字符串 "yyyy-MM-dd"
//6、将转换后的字符串放置header中
//7、返回event
    public Event intercept(Event event) {
        //获取event的body
        String eventBody = new String(event.getBody(), Charsets.UTF_8);

        //获取event的header
        Map<String, String> headersMap = event.getHeaders();

        String[] bodyArr = eventBody.split("\\s+");

        try {
            String jsonStr = bodyArr[6];
            String timestampStr = "";
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            byte[] bodyBytes = jsonStr.getBytes();

            //解析json获取时间戳
            if (headersMap.getOrDefault("logtype", "").equals("start")) {
                timestampStr = jsonObject.getJSONObject("app_active").getString("time");
            } else if (headersMap.getOrDefault("logtype", "").equals("event")) {
                JSONArray jsonArray = jsonObject.getJSONArray("lagou_event");
                if (jsonArray.size() > 0) {
                    timestampStr = jsonArray.getJSONObject(0).getString("time");
                }

            }


            //将时间戳转换为字符串“yyyy-MM-dd”

            //将字符串转换为Long
            long timestamp = Long.parseLong(timestampStr);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

            Instant instant = Instant.ofEpochMilli(timestamp);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            String date = formatter.format(localDateTime);

            //将转换后的字符串放入header
            headersMap.put("logtime", date);
            event.setHeaders(headersMap);
            event.setBody(bodyBytes);

        } catch (Exception e) {
            headersMap.put("logtime", "unknown");
            event.setHeaders(headersMap);
        }


        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> lstEvent = new ArrayList<>();

        for (Event event : events) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                lstEvent.add(outEvent);
            }

        }

        return lstEvent;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new HomeworkInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

    @Test
    public void testJunit() {

//        String str = "2020-08-02 18:19:32.959 [main] INFO com.lagou.ecommerce.AppStart - {\"app_active\":{\"name\":\"app_active\",\"json\":{\"entry\":\"1\",\"action\":\"0\",\"error_code\":\"0\"},\"time\":1596342840284},\"attr\":{\"area\":\"大庆\",\"uid\":\"2F10092A2\",\"app_v\":\"1.1.15\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1002\",\"os_type\":\"2.8\",\"channel\":\"TB\",\"language\":\"chinese\",\"brand\":\"iphone-8\"}}";
        String str = "2021-09-16 16:54:58.166 [main] INFO  com.lagou.ecommerce.AppEvent - {\"lagou_event\":[{\"name\":\"goods_detail_loading\",\"json\":{\"entry\":\"2\",\"goodsid\":\"0\",\"loading_time\":\"58\",\"action\":\"2\",\"staytime\":\"13\",\"showtype\":\"0\"},\"time\":1596124800000}],\"attr\":{\"area\":\"遵义\",\"uid\":\"2F10092A2\",\"app_v\":\"1.1.14\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1002\",\"os_type\":\"7.6.2\",\"channel\":\"HK\",\"language\":\"chinese\",\"brand\":\"iphone-3\"}}\n";

        HashMap<String, String> map = new HashMap<>();
        map.put("logtype", "event");
        // new events
        SimpleEvent event = new SimpleEvent();
        event.setHeaders(map);
        event.setBody(str.getBytes(Charsets.UTF_8));

        //调用interceptor 处理event

        HomeworkInterceptor customerInterceptor = new HomeworkInterceptor();
        Event outEvent = customerInterceptor.intercept(event);
        String eventBody = new String(outEvent.getBody(), Charsets.UTF_8);
        Map<String, String> headersMap = outEvent.getHeaders();
        System.out.println(eventBody);

        //返回处理结果

    }


}
