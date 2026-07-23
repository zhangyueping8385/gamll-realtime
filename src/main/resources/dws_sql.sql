create database gmall_realtime;

drop table if exists gmall_realtime.dws_traffic_source_keyword_page_view_window;
create table if not exists gmall_realtime.dws_traffic_source_keyword_page_view_window (
  `stt` DATETIME COMMENT '窗口起始时间',
  `edt` DATETIME COMMENT '窗口结束时间',
  `cur_date` DATE COMMENT '当天日期',
  `keyword` VARCHAR(128) COMMENT '搜索关键词',
    `keyword_count` BIGINT REPLACE COMMENT '搜索关键词出现次数'
    ) engine = olap
    aggregate key (`stt`, `edt`, `cur_date`, `keyword`)
    partition by range(`cur_date`)()
    distributed by hash(`stt`) buckets 10
    properties (
       "replication_num" = "1",
       "dynamic_partition.enable" = "true",
       "dynamic_partition.time_unit" = "DAY",
       "dynamic_partition.end" = "3",
       "dynamic_partition.prefix" = "par",
       "dynamic_partition.buckets" = "10"
    );


drop table if exists gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window;
create table if not exists gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window
(
    `stt`      DATETIME COMMENT '窗口起始时间',
    `edt`      DATETIME COMMENT '窗口结束时间',
    `cur_date` DATE COMMENT '当天日期',
    `vc`       VARCHAR(256) COMMENT '版本号',
    `ch`       VARCHAR(256) COMMENT '渠道',
    `ar`       BIGINT COMMENT '地区',
    `is_new`   TINYINT COMMENT '新老访客状态标记',
    `uv_ct`    BIGINT REPLACE COMMENT '独立访客数',
    `sv_ct`    BIGINT REPLACE COMMENT '会话数',
    `pv_ct`    BIGINT REPLACE COMMENT '页面浏览数',
    `dur_sum`  BIGINT REPLACE COMMENT '累计访问时长'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`,`vc`,`ch`,`ar`,`is_new`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);


drop table if exists gmall_realtime.dws_traffic_home_detail_page_view_window;
create table if not exists gmall_realtime.dws_traffic_home_detail_page_view_window
(
    `stt`               DATETIME COMMENT '窗口起始时间',
    `edt`               DATETIME COMMENT '窗口结束时间',
    `cur_date`          DATE COMMENT '当天日期',
    `home_uv_ct`        BIGINT REPLACE COMMENT '首页独立访客数',
    `good_detail_uv_ct` BIGINT REPLACE COMMENT '商品详情页独立访客数'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);



drop table if exists gmall_realtime.dws_user_user_login_window;
create table if not exists gmall_realtime.dws_user_user_login_window
(
    `stt`      DATETIME COMMENT '窗口起始时间',
    `edt`      DATETIME COMMENT '窗口结束时间',
    `cur_date` DATE COMMENT '当天日期',
    `back_ct`  BIGINT REPLACE COMMENT '回流用户数',
    `uu_ct`    BIGINT REPLACE COMMENT '独立用户数'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);



drop table if exists gmall_realtime.dws_user_user_register_window;
create table if not exists gmall_realtime.dws_user_user_register_window
(
    `stt`         DATETIME COMMENT '窗口起始时间',
    `edt`         DATETIME COMMENT '窗口结束时间',
    `cur_date`    DATE COMMENT '当天日期',
    `register_ct` BIGINT REPLACE COMMENT '注册用户数'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);


drop table if exists gmall_realtime.dws_trade_cart_add_uu_window;
create table if not exists gmall_realtime.dws_trade_cart_add_uu_window
(
    `stt`            DATETIME COMMENT '窗口起始时间',
    `edt`            DATETIME COMMENT '窗口结束时间',
    `cur_date`       DATE COMMENT '当天日期',
    `cart_add_uu_ct` BIGINT REPLACE COMMENT '加购独立用户数'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);


drop table if exists gmall_realtime.dws_trade_payment_suc_window;
create table if not exists gmall_realtime.dws_trade_payment_suc_window
(
    `stt`                           DATETIME COMMENT '窗口起始时间',
    `edt`                           DATETIME COMMENT '窗口结束时间',
    `cur_date`                      DATE COMMENT '当天日期',
    `payment_suc_unique_user_count` BIGINT REPLACE COMMENT '支付成功独立用户数',
    `payment_suc_new_user_count`    BIGINT REPLACE COMMENT '支付成功新用户数'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);

drop table if exists gmall_realtime.dws_trade_order_window;
create table if not exists gmall_realtime.dws_trade_order_window
(
    `stt`                     DATETIME COMMENT '窗口起始时间',
    `edt`                     DATETIME COMMENT '窗口结束时间',
    `cur_date`                DATE COMMENT '当天日期',
    `order_unique_user_count` BIGINT REPLACE COMMENT '下单独立用户数',
    `order_new_user_count`    BIGINT REPLACE COMMENT '下单新用户数'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-30",
"dynamic_partition.end" = "7",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);


drop table if exists gmall_realtime.dws_trade_sku_order_window;
create table if not exists gmall_realtime.dws_trade_sku_order_window
(
    `stt`                    DATETIME COMMENT '窗口起始时间',
    `edt`                    DATETIME COMMENT '窗口结束时间',
    `cur_date`               DATE COMMENT '当天日期',
    `trademark_id`           SMALLINT COMMENT '品牌ID',
    `trademark_name`         CHAR(255) COMMENT '品牌名称',
    `category1_id`           SMALLINT COMMENT '一级品类ID',
    `category1_name`         CHAR(128) COMMENT '一级品类名称',
    `category2_id`           SMALLINT COMMENT '二级品类ID',
    `category2_name`         CHAR(128) COMMENT '二级品类名称',
    `category3_id`           SMALLINT COMMENT '三级品类ID',
    `category3_name`         CHAR(128) COMMENT '三级品类名称',
    `sku_id`                 INT COMMENT 'SKU_ID',
    `sku_name`               CHAR(255) COMMENT 'SKU名称',
    `spu_id`                 INT COMMENT 'SPU_ID',
    `spu_name`               CHAR(255) COMMENT 'SPU名称',
    `original_amount`        DECIMAL(16, 2) REPLACE COMMENT '原始金额',
    `activity_reduce_amount` DECIMAL(16, 2) REPLACE COMMENT '活动减免金额',
    `coupon_reduce_amount`   DECIMAL(16, 2) REPLACE COMMENT '优惠券减免金额',
    `order_amount`           DECIMAL(16, 2) REPLACE COMMENT '下单金额'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`,`trademark_id`,`trademark_name`,`category1_id`,`category1_name`,`category2_id`,`category2_name`,`category3_id`,`category3_name`,`sku_id`,`sku_name`,`spu_id`,`spu_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);


drop table if exists gmall_realtime.dws_trade_province_order_window;
create table if not exists gmall_realtime.dws_trade_province_order_window
(
    `stt`           DATETIME COMMENT '窗口起始时间',
    `edt`           DATETIME COMMENT '窗口结束时间',
    `cur_date`      DATE COMMENT '当天日期',
    `province_id`   TINYINT COMMENT '省份ID',
    `province_name` CHAR(128) COMMENT '省份名称',
    `order_count`   BIGINT REPLACE COMMENT '累计下单次数',
    `order_amount`  DECIMAL(16, 2) REPLACE COMMENT '累计下单金额'
) engine = olap aggregate key (`stt`, `edt`,`cur_date`,`province_id`,`province_name`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);