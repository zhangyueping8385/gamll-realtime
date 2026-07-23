package org.example.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.example.realtime.bean.TradeSkuOrderBean;

public interface DimJoinFunction<T> {
    public abstract String getId(T tradeSkuOrderBean);
    public abstract String getTableName();
    public abstract void join(T tradeSkuOrderBean, JSONObject dim);
}
