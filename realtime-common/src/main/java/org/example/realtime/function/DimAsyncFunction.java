package org.example.realtime.function;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.example.realtime.bean.TradeSkuOrderBean;
import org.example.realtime.constant.Constant;
import org.example.realtime.util.HbaseUtil;
import org.example.realtime.util.RedisUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;
    String tableName;

    public DimAsyncFunction() {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HbaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(T tradeSkuOrderBean, ResultFuture<T> resultFuture) throws Exception {
        String rowKey = getId(tradeSkuOrderBean);
        String tableName = getTableName();
        String redisKey = RedisUtil.getRedisKey(tableName, rowKey);
        // java的异步IO方式
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                // 第一步异步访问的值
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(redisKey);
                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();

                } catch (Exception e) {
                    e.printStackTrace();
                }
                return dimInfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJsonObj = null;
                // 旁路缓存判断
                if (dimInfo == null || dimInfo.length() == 0) {
                    // 需要访问hbase
                    try {
                        dimJsonObj = HbaseUtil.putAsyncRow(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                        // 缓存到redis
                        redisAsyncConnection.async().setex(redisKey, 24 * 60 * 60, dimJsonObj.toJSONString());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // redis存在缓存数据
                    dimJsonObj = JSONObject.parseObject(dimInfo);
                }
                return dimJsonObj;

            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dim) {
                if (dim == null) {
                    // 无法关联当前的纬度信息
                    System.out.println("无法关联当前的纬度信息，skuId：" + rowKey);
                } else {
                    join(tradeSkuOrderBean, dim);
//                    System.out.println(dim.toJSONString());
                }
                // 合并纬度信息，返回结果
                resultFuture.complete(Collections.singletonList(tradeSkuOrderBean));
            }
        });
    }


}
