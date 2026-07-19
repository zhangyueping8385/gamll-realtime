package org.example.realtime.dws.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.realtime.base.BaseSQLAPP;
import org.example.realtime.constant.Constant;
import org.example.realtime.dws.app.function.KwSplit;
import org.example.realtime.util.SQLUtil;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().Start(10021,4,Constant.DORIS_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupID) {
        //1. 创建page_info表，用于存储dwd_traffic_page表中的数据
        tableEnv.executeSql("create table page_info(\n" +
                "\t`common` map<string,string>,\n" +
                "\t`page` map<string,string>,\n" +
                "\t`ts` bigint,\n" +
                "\t`row_time` as to_timestamp_ltz(ts,3),\n" +  // 去掉多余右括号，逗号正常分隔
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" + // 修正字段名 row_time
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,ckAndGroupID));

        //2. 从page_info表中查询搜索页和关键词页的点击事件
        Table keywordPageInfoTable = tableEnv.sqlQuery("select\n" +
                "\tpage['item'] keywords,\n" +
                "\trow_time\n" +
                "from\n" +
                "\tpage_info\n" +
                "where\n" +
                "\t`page`['last_page_id'] = 'search'\n" +
                "\tand `page`['item_type'] = 'keyword'\n" +
                "\tand `page`['item'] is not null");
        tableEnv.createTemporaryView("keywordPageInfo", keywordPageInfoTable);

        //3. 自定义函数KwSplit，用于对关键词进行分词，并注册
        tableEnv.createTemporarySystemFunction("SplitFunction", KwSplit.class);

        //4. 调用分词函数对keywords进行拆分 keywords表示视图中的keywords字段，keyword表示分词后的关键词
        Table keywordTable = tableEnv.sqlQuery("SELECT keywords, keyword,`row_time`" +
                "FROM keywordPageInfo, LATERAL TABLE(SplitFunction(keywords))");
        tableEnv.createTemporaryView("keywordTable", keywordTable);

        //5. 对keyword进行分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("SELECT\n" +
                "  cast(TUMBLE_START(row_time, INTERVAL '10' SECOND) as string) AS wt_start,\n" +
                "  cast(TUMBLE_END(row_time, INTERVAL '10' SECOND) as string) AS wt_end,\n" +
                "  cast(CURRENT_DATE as string) AS cur_date,\n" +
                "  keyword,\n" +
                "  COUNT(keyword) keyword_count\n" +
                "FROM \n" +
                "\tkeywordTable\n" +
                "GROUP BY\n" +
                "  TUMBLE(row_time, INTERVAL '10' SECOND),\n" +
                "  keyword");

        //6.写出到doris表
        tableEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                "    ) \n"+SQLUtil.getDorisSinkSQL(Constant.DORIS_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));

        resultTable.insertInto(Constant.DORIS_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW).execute();
        System.out.println("成功写入Doris表：" + Constant.DORIS_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);

    }
}
