package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class TestBaseFieldUDF extends UDF {
    public String evaluate(String line , String jsonkeysString){
        //0. 准备一个StringBuilder
        StringBuilder sb = new StringBuilder();

        //1.切割json
        String[] jsonkeys = jsonkeysString.split(",");
        //2.处理line
        String[] logContents = line.split("\\|");

        //合法性校验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])){
            return "";
        }
        //处理json
        try{
            JSONObject jsonObject = new JSONObject(logContents[1]);
            //获取cm里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");
            for (int i = 0 ;i<logContents.length; i++){
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
}
