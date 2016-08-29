package org.tonypacer.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.tonypacer.domain.Task;

/**
 * Created by XuanYu on 2016/6/4.
 */
public class ParamUtils {

    /**
     * 从命令行参数中提取任务id
     * @param args 命令行参数
     * @return 任务id
     */
    public static Long getTaskIdFromArgs(String[] args) {
        try {
            if(args != null && args.length > 0) {
                return Long.valueOf(args[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null ;
    }

    /**
     * 依据Task获取参数
     * @param task
     * @return
     */
    public static JSONObject getTaskParam(Task task) {
        JSONObject  taskParam = JSONObject.parseObject(task.getTaskParam()) ;
        if(taskParam != null && taskParam.isEmpty()) {
            return taskParam;
        }
        return null;
    }

    /**
     * 从JSON对象中提取参数
     * @param jsonObject JSON对象
     * @return 参数
     */
    public static String getParam(JSONObject jsonObject, String field) {
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if(jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        return null;
    }
}
