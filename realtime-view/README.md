# realtime-view

GMALL 实时数仓的 Spring Boot 可视化模块：通过 JDBC 查询 Doris，将 DWS 汇总数据转换为实时大屏所需的指标和图表数据。

## 功能

- 首页实时指标总览：用户注册、订单、金额、PV、支付和加购。
- 指标趋势：注册、订单和 PV 的时间趋势。
- TopN、分布、全国省份订单地图和支付成功率数据接口。
- Thymeleaf 页面渲染，ECharts 负责图表展示。

## 技术栈

- Spring Boot 2.7.18
- Java 8
- Spring Web、Spring JDBC、Thymeleaf
- MySQL JDBC Driver（Doris MySQL 协议）
- ECharts 5.4.0、echarts-wordcloud
- Maven

## 目录结构

```text
realtime-view/
├── src/main/java/org/example/realtime/view/
│   ├── config/
│   │   ├── DataSourceConfig.java
│   │   └── DorisConfig.java
│   ├── controller/
│   │   ├── DataController.java
│   │   └── PageController.java
│   ├── service/
│   │   ├── DorisQueryService.java
│   │   └── DashboardService.java
│   └── ViewApplication.java
├── src/main/resources/
│   ├── templates/dashboard.html
│   └── application.yml
├── start.sh
└── pom.xml
```

## 配置

默认配置位于 `src/main/resources/application.yml`：

```yaml
server:
  port: 8080

doris:
  fenodes: ${DORIS_FENODES:bigdata1:9030}
  database: ${DORIS_DATABASE:gmall_realtime}
  username: ${DORIS_USERNAME:root}
  password: ${DORIS_PASSWORD:}
```

也可以通过环境变量配置：

```bash
export DORIS_FENODES=bigdata1:9030
export DORIS_DATABASE=gmall_realtime
export DORIS_USERNAME=root
export DORIS_PASSWORD='你的密码'
```

应用使用 Doris 的 MySQL 协议端口查询数据；Flink 写入 Doris 时使用的 FE 地址由 `realtime-common` 中的常量配置，两个地址请按实际部署核对。

## 启动

在仓库根目录构建：

```bash
mvn clean package -DskipTests
```

启动开发环境：

```bash
cd realtime-view
mvn spring-boot:run
```

或运行打包文件：

```bash
cd realtime-view
mvn clean package -DskipTests
java -jar target/realtime-view-1.0-SNAPSHOT.jar
```

启动后访问：<http://localhost:8080/>

`start.sh` 会先执行编译再启动，但脚本内包含本机绝对路径，跨机器使用前请先修改；通常推荐直接使用 Maven 命令。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/dashboard/overall` | 获取总览指标 |
| `GET` | `/api/dashboard/trend?metric={metric}&timeRange={timeRange}` | 获取趋势数据；`metric` 支持 `user_register`、`order_count`、`page_view` |
| `GET` | `/api/dashboard/map` | 获取省份订单数据 |
| `GET` | `/api/dashboard/payment-success-rate` | 获取支付成功率 |
| `GET` | `/api/charts/topN?tableName=...&groupBy=...&aggField=...&aggFunc=...&topN=10` | 获取 TopN 聚合数据 |
| `GET` | `/api/charts/distribution?tableName=...&groupBy=...&valueField=...` | 获取分布聚合数据 |

当前版本只实现首页大屏和上述数据接口，未实现独立的表浏览、在线 SQL 编辑器或 `/tables`、`/query`、`/charts` 页面。

## 依赖的数据表

大屏默认查询以下 Doris DWS 表：

- `dws_user_user_register_window`
- `dws_trade_order_window`
- `dws_trade_province_order_window`
- `dws_traffic_vc_ch_ar_is_new_page_view_window`
- `dws_trade_payment_suc_window`
- `dws_trade_cart_add_uu_window`

因此，需要先启动对应的 Flink DWS 任务并确保 `gmall_realtime` 中已有数据。

## 日志与排查

- 日志文件：`logs/realtime-view.log`
- 连接失败：检查 `DORIS_FENODES`、数据库名、账号密码和 Doris MySQL 协议端口。
- 页面无数据：检查对应 DWS 表是否存在、是否有数据，以及 DWS 任务是否正常运行。
- 图表资源加载失败：检查网络是否可以访问 jsDelivr；离线部署时改用本地静态资源。
