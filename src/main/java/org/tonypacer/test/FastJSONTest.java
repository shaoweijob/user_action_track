package org.tonypacer.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by XuanYu on 2016/6/4.
 */
public class FastJSONTest {

    public static void main(String[] args) {
        // 测试数据
        String json = "[{'学生':'张三', '班级':'一班', '年级':'大一', '科目':'高数', '成绩':90}, {'学生':'李四', '班级':'二班', '年级':'大一', '科目':'高数', '成绩':80}]";

        // create JSONArray Object
        JSONArray jsonArray = JSONArray.parseArray(json);
        // create JSONObject Object
        JSONObject jsonObject = jsonArray.getJSONObject(0);
        // Get Value By Key
        System.out.println(jsonObject.getString("学生"));
    }

}
