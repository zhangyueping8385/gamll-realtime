package org.example.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T value) throws Exception {
        SerializeConfig config = new SerializeConfig(); // 配置json序列化策略,将属性名转换为驼峰命名法
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase); // 驼峰命名法转换为下划线命名法
        return JSONObject.toJSONString(value, config); // 将对象转换为json字符串
    }
}
