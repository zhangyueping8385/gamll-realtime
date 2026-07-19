package org.example.realtime.dws.app.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.example.realtime.util.IkUtil;

import java.util.List;

// 注解：指定输出数据的类型，包含字段 keyword 和 length
@FunctionHint(output = @DataTypeHint("ROW<keyword STRING, length INT>"))

public class KwSplit extends TableFunction<Row> {
    // 评估方法：对输入字符串进行分词并生成 Row 对象
    public void eval(String str) {
        List<String> keywords = IkUtil.IkSplit(str);
        for (String keyword : keywords) {
            collect(Row.of(keyword, keyword.length()));
        }

    }
}
