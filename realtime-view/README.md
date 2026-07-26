# 实时数据可视化模块

## 概述

实时数据可视化模块是gmall-realtime项目的一个子模块，用于通过Web界面可视化展示Doris中的实时数据。该模块基于Spring Boot框架开发，提供丰富的图表展示和数据查询功能。

## 技术架构

- **后端框架**: Spring Boot 2.7.18
- **前端技术**: HTML5 + Bootstrap 5 + ECharts 5.4.0
- **数据访问**: JDBC直连Doris
- **模板引擎**: Thymeleaf
- **构建工具**: Maven

## 功能特性

### 1. 仪表板概览
- 实时显示关键业务指标
- 用户注册/登录趋势图
- 订单和支付统计
- 热门搜索关键词排行
- 渠道访问分布

### 2. 数据表浏览
- 查看所有DWS表结构
- 分页浏览表数据
- 表结构信息展示

### 3. SQL查询
- 在线SQL编辑器
- 查询结果表格展示
- 常用查询示例
- 查询历史记录

### 4. 图表展示
- 多种图表类型（折线图、柱状图、饼图）
- 用户行为分析图表
- 交易数据分析图表
- 流量分析图表

## 项目结构

```
realtime-view/
├── src/main/java/org/example/realtime/view/
│   ├── config/                 # 配置类
│   │   ├── DorisConfig.java   # Doris连接配置
│   │   └── DataSourceConfig.java # 数据源配置
│   ├── controller/             # 控制器
│   │   ├── DataController.java # 数据API接口
│   │   └── PageController.java # 页面控制器
│   ├── service/               # 服务层
│   │   ├── DorisQueryService.java # Doris查询服务
│   │   └── DashboardService.java  # 仪表板服务
│   └── ViewApplication.java   # 启动类
├── src/main/resources/
│   ├── templates/             # HTML页面模板
│   │   ├── dashboard.html     # 仪表板页面
│   │   ├── tables.html        # 数据表页面
│   │   ├── query.html         # SQL查询页面
│   │   └── charts.html        # 图表展示页面
│   └── application.yml        # 应用配置
└── pom.xml                    # Maven配置
```

## 配置说明

### application.yml配置

```yaml
# Doris配置
doris:
  fenodes: bigdata1:8030          # Doris FE节点地址
  database: gmall_realtime        # Doris数据库名
  username: root                  # Doris用户名
  password: ""                    # Doris密码

# 服务器配置
server:
  port: 8080                      # 应用端口
```

### 环境变量配置

支持通过环境变量配置Doris连接信息：
- `DORIS_FENODES`: Doris FE节点地址
- `DORIS_DATABASE`: Doris数据库名
- `DORIS_USERNAME`: Doris用户名
- `DORIS_PASSWORD`: Doris密码

## API接口

### 仪表板相关
- `GET /api/dashboard/overview` - 获取仪表板概览数据
- `GET /api/dashboard/trend?metric={metric}&timeRange={timeRange}` - 获取趋势数据

### 数据表相关
- `GET /api/tables` - 获取所有可用表
- `GET /api/tables/{tableName}/info` - 获取表结构信息
- `GET /api/tables/{tableName}/data?page={page}&size={size}` - 查询表数据

### 查询相关
- `POST /api/query` - 执行SQL查询（JSON格式：{"sql": "SELECT * FROM table"}）
- `GET /api/charts/summary` - 获取图表汇总数据

## 启动方式

### 方式1：使用启动脚本
```bash
cd /Users/xiaozhang/Code/JavaCode/实时数仓4.0/gmall-realtime/realtime-view
./start.sh
```

### 方式2：使用Maven命令
```bash
cd /Users/xiaozhang/Code/JavaCode/实时数仓4.0/gmall-realtime/realtime-view
mvn spring-boot:run
```

### 方式3：打包后运行
```bash
cd /Users/xiaozhang/Code/JavaCode/实时数仓4.0/gmall-realtime/realtime-view
mvn clean package
java -jar target/realtime-view-1.0-SNAPSHOT.jar
```

## 访问地址

应用启动后，可以通过以下地址访问：

- **仪表板**: http://localhost:8080/
- **数据表浏览**: http://localhost:8080/tables
- **SQL查询**: http://localhost:8080/query
- **图表展示**: http://localhost:8080/charts

## 支持的DWS表

基于Constant.java中的配置，支持以下DWS表的可视化：

- `dws_traffic_source_keyword_page_view_window` - 搜索关键词页面浏览
- `dws_traffic_vc_ch_ar_is_new_page_view_window` - 版本渠道地区访客页面浏览
- `dws_traffic_home_detail_page_view_window` - 首页详情页页面浏览
- `dws_user_user_login_window` - 用户登录
- `dws_user_user_register_window` - 用户注册
- `dws_trade_cart_add_uu_window` - 加购统计
- `dws_trade_payment_suc_window` - 支付成功
- `dws_trade_order_window` - 订单统计
- `dws_trade_sku_order_window` - SKU粒度订单
- `dws_trade_province_order_window` - 省份粒度订单

## 使用示例

### 1. 查看仪表板
访问 http://localhost:8080/ 可以看到实时数据概览，包括：
- 用户注册和登录总数
- 订单和支付总数
- 热门搜索关键词
- 渠道访问分布

### 2. 浏览数据表
访问 http://localhost:8080/tables 可以：
- 选择要查看的DWS表
- 查看表结构信息
- 分页浏览表数据

### 3. 执行SQL查询
访问 http://localhost:8080/query 可以：
- 在SQL编辑器中输入查询语句
- 执行查询并查看结果
- 使用提供的示例查询

### 4. 查看图表
访问 http://localhost:8080/charts 可以看到：
- 用户行为趋势图表
- 交易分析图表
- 流量分析图表
- 商品销售排行

## 注意事项

1. **安全性**: 当前版本仅允许执行SELECT查询，不允许执行INSERT、UPDATE、DELETE等操作
2. **性能**: 建议对大数据量查询使用LIMIT限制结果集大小
3. **连接**: 确保Doris服务正常运行且网络连通
4. **权限**: 确保Doris用户具有相应的查询权限

## 扩展建议

1. **用户认证**: 可以添加登录认证功能
2. **查询缓存**: 对频繁查询的结果进行缓存
3. **导出功能**: 支持查询结果导出为CSV、Excel等格式
4. **图表定制**: 允许用户自定义图表类型和样式
5. **告警功能**: 基于数据阈值设置告警规则
6. **移动端适配**: 优化移动端显示效果

## 故障排查

### 常见问题

1. **连接失败**: 检查Doris服务状态和连接配置
2. **查询超时**: 优化SQL语句，添加适当的过滤条件
3. **数据显示异常**: 检查数据类型和格式转换
4. **图表不显示**: 检查浏览器控制台错误信息

### 日志查看

应用日志位于：`logs/realtime-view.log`

可以通过日志查看详细的错误信息和运行状态。