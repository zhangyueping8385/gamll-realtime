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
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    /** DWS 任务输出的标准化窗口指标事件。 */
    public static final String TOPIC_DWS_METRIC_WINDOW = "dws_metric_window";
    /** 实时异常检测任务输出的告警事件。 */
    public static final String TOPIC_ADS_METRIC_ANOMALY = "ads_metric_anomaly";


    public static final String DORIS_FENODES = "bigdata1:8030";
    public static final String DORIS_DATABASE = "gmall_realtime";
    public static final String DORIS_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    public static final String DORIS_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DORIS_DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW = "dws_traffic_home_detail_page_view_window";
    public static final String DORIS_DWS_USER_USER_LOGIN_WINDOW = "dws_user_user_login_window";
    public static final String DORIS_DWS_USER_USER_REGISTER_WINDOW = "dws_user_user_register_window";
    public static final String DORIS_DWS_TRADE_CART_ADD_UU_WINDOW = "dws_trade_cart_add_uu_window";
    public static final String DORIS_DWS_TRADE_PAYMENT_SUC_WINDOW = "dws_trade_payment_suc_window";
    public static final String DORIS_DWS_TRADE_ORDER_WINDOW = "dws_trade_order_window";
    public static final String DORIS_DWS_TRADE_SKU_ORDER_WINDOW_SYNC_CACHE = "dws_trade_sku_order_window";
    public static final String DORIS_DWS_TRADE_PROVINCE_ORDER_WINDOW = "dws_trade_province_order_window";
    public static final String DORIS_ADS_METRIC_ANOMALY = "ads_metric_anomaly";
    public static final String DORIS_ADS_METRIC_ANOMALY_ANALYSIS = "ads_metric_anomaly_analysis";
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
    public static final String HBASE_TABLE_DIM_SKU_INFO = "dim_sku_info";
    public static final String HBASE_TABLE_DIM_SPU_INFO = "dim_spu_info";
    public static final String HBASE_TABLE_DIM_BASE_CATEGORY3 = "dim_base_category3";
    public static final String HBASE_TABLE_DIM_BASE_CATEGORY2 = "dim_base_category2";
    public static final String HBASE_TABLE_DIM_BASE_CATEGORY1 = "dim_base_category1";
    public static final String HBASE_TABLE_DIM_BASE_TRADEMARK = "dim_base_trademark";
    public static final String HBASE_ZOOKEEPER_QUORUM = "bigdata1,bigdata2,bigdata3";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
    public static final String HBASE_ZOOKEEPER_AND_PORT_QUORUM = "bigdata1:2181";
    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-fully";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";


    public static final Integer REDIS_TWO_DAY_SECONDS = 17800;
    public static final String REDIS_HOST = "bigdata1";
    public static final Integer REDIS_PORT = 6379;
}
