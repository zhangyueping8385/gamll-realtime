package org.example.realtime.constant;

public class Constant {

    public static final String KAFKA_BROKERS = "bigdata1:9092,bigdata2:9092,bigdata3:9092";
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String TOPIC_DWD_INTERACTION_COMMON_INFO = "dwd_interaction_common_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL = "dwd_trade_order_cancel_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL = "dwd_trade_order_pay_suc_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_BASE_DB = "dwd_base_db";


    public static final String DORIS_FENODES = "bigdata1:8030";
    public static final String DORIS_DATABASE = "gmall_realtime";
    public static final String DORIS_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    public static final String DORIS_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "";



    public static final String MYSQL_HOST = "bigdata1";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USERNAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://bigdata1:3306?useSSL=false&characterEncoding=utf8";
    public static final String MYSQL_DIM_PROCESS_DATABASE = "gmall_config";
    public static final String MYSQL_DIM_PROCESS_TABLE = "table_process_dim";
    public static final String MYSQL_DWD_PROCESS_TABLE = "table_process_dwd";

    public static final String HBASE_NAMESPACE = "gmall";
    public static final String HBASE_ZOOKEEPER_QUORUM = "bigdata1,bigdata2,bigdata3";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
    public static final String HBASE_ZOOKEEPER_AND_PORT_QUORUM = "bigdata1:2181";
    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-fully";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
}
