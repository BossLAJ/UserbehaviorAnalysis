package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line,String jsonkeysString){
        //0.准备一个sb
        StringBuilder sb = new StringBuilder();

        //1.切割jsonkeys
        String[] jsonkeys = jsonkeysString.split(",");

        //2.处理line    服务器时间 | json
        String[] logContents = line.split("\\|");

        //3. 合法性校验
        if (logContents.length !=2 || StringUtils.isBlank(logContents[1])){
            return "";
        }

        //4.开始处理json
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);

            //获取cm里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");

            //循环遍历取值
            for (int i = 0; i<jsonkeys.length; i++){
                String fileName = jsonkeys[i].trim();
                if (base.has(fileName)){
                    sb.append(base.getString(fileName)).append("\t");
                }else {
                    sb.append("").append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
//
//    public static void main(String[] args) {
//        String line = "1577808106052|{\"cm\":{\"ln\":\"-79.6\",\"sv\":\"V2.4.4\",\"os\":\"8.2.5\",\"g\":\"ZLY09LT3@gmail.com\",\"mid\":\"m528\",\"nw\":\"3G\",\"l\":\"en\",\"vc\":\"12\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"u695\",\"t\":\"1577780985211\",\"la\":\"-20.3\",\"md\":\"Huawei-1\",\"vn\":\"1.1.7\",\"ba\":\"Huawei\",\"sr\":\"L\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1577764701806\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"1\",\"newsid\":\"n135\",\"news_staytime\":\"30\",\"loading_time\":\"36\",\"action\":\"4\",\"showtype\":\"3\",\"category\":\"1\",\"type1\":\"\"}},{\"ett\":\"1577793685061\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"2\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"201\",\"loading_way\":\"1\"}}]}";
//        String x = new BaseFieldUDF().evaluate(line,"mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
//        System.out.println(x);
//    }
//
}
