package org.example.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.example.realtime.constant.Constant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * HBase工具类
 * 提供HBase连接管理、表创建、表删除等常用操作
 */
public class HbaseUtil {

    /**
     * 获取HBase连接
     *
     * @return HBase连接对象，如果创建失败返回null
     */
    public static Connection getConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        conf.set("hbase.zookeeper.property.clientPort", Constant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
        // 关键：Zookeeper中HBase的znode路径（根据集群实际配置为/hbase-fully）
        conf.set("zookeeper.znode.parent", Constant.ZOOKEEPER_ZNODE_PARENT);

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            // 连接创建后立即验证：获取Admin会触发实际的Master连接
            try (Admin admin = connection.getAdmin()) {
                String[] namespaces = admin.listNamespaces();
                System.out.println("[HBase] 连接成功！现有命名空间: " + String.join(", ", namespaces));
            } catch (IOException e) {
                System.err.println("[HBase] ERROR: Zookeeper可连但Master不可达");
                System.err.println("[HBase] 请检查：1. bigdata1 16000端口  2. HMaster进程  3. 服务器端hosts配置  4. znode路径是否正确");
                throw e;
            }
        } catch (IOException e) {
            System.err.println("[HBase] ERROR: 创建连接失败 - " + e.getMessage());
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        conf.set("hbase.zookeeper.property.clientPort", Constant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
        conf.set("zookeeper.znode.parent", Constant.ZOOKEEPER_ZNODE_PARENT);

        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 异步的从 hbase 读取维度数据
     *
     * @param hBaseAsyncConn hbase 的异步连接
     * @param nameSpace      命名空间
     * @param tableName      表名
     * @param rowKey         rowKey
     * @return 读取到的维度数据, 封装到 json 对象中.
     */
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                          String nameSpace,
                                          String tableName,
                                          String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
                .getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                dim.put(key, value);
            }

            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    /**
     * 创建HBase表
     *
     * @param connection HBase连接对象
     * @param nameSpace  命名空间名称
     * @param table      表名
     * @param families   列族名称列表，可变参数，至少需要一个列族
     * @throws IOException 当创建表失败时抛出
     */
    public static void CreateTable(Connection connection, String nameSpace, String table, String... families) throws IOException {
        if (families == null || families.length == 0) {
            return;
        }

        TableName tableName = TableName.valueOf(nameSpace, table);

        try (Admin admin = connection.getAdmin()) {
            // 检查命名空间是否存在，不存在则创建
            try {
                admin.getNamespaceDescriptor(nameSpace);
            } catch (IOException e) {
                System.out.println("命名空间不存在，自动创建: " + nameSpace);
                admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
            }

            // 检查表是否已存在，避免重复创建
            if (admin.tableExists(tableName)) {
                System.out.println("表已存在，无需创建: " + tableName);
                return;
            }

            // 创建表格描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

            for (String family : families) {
                // 创建列族描述器并添加到表描述器中
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表创建成功: " + tableName);
        } catch (IOException e) {
            System.err.println("创建表失败: " + tableName + ", 错误: " + e.getMessage());
            throw e;
        }
    }

    /**
     * 删除HBase表
     *
     * @param connection HBase连接对象
     * @param nameSpace  命名空间名称
     * @param table      表名
     * @throws Exception 当删除表失败时抛出
     */
    public static void dropTable(Connection connection, String nameSpace, String table) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            // 先禁用表，禁用后才能删除
            admin.disableTable(TableName.valueOf(nameSpace, table));
            // 删除表
            admin.deleteTable(TableName.valueOf(nameSpace, table));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putCells(Connection connection, String nameSpace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        // 打开表
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        // 创建Put操作对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 遍历JSON数据，添加到Put操作中，每个JSON键值对对应一个列，列名是JSON键，列值是JSON值
        for (String column : data.keySet()) {
            String dataString = data.getString(column);
            if (dataString != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(dataString));
            }
        }

        // 执行Put操作
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭表
        table.close();
    }

    /**
     * 根据参数从 hbase 指定的表中查询一行数据
     *
     * @param hbaseConn hbase 链接
     * @param nameSpace 命名空间
     * @param table     表面
     * @param rowKey    rowKey
     * @return 把一行查询到的所有列封装到一个 JSONObject 对象中
     */
    public static <T> T getRow(Connection hbaseConn,
                               String nameSpace,
                               String table,
                               String rowKey,
                               Class<T> tClass,
                               boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        try (Table Table = hbaseConn.getTable(TableName.valueOf(nameSpace, table))) { // jdk1.7 : 可以自动释放资源
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = Table.get(get);
            // 4. 把查询到的一行数据,封装到一个对象中: JSONObject
            // 4.1 一行中所有的列全部解析出来
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            T t = tClass.newInstance();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (defaultIsUToC) { // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                    key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                }
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                BeanUtils.setProperty(t, key, value);
            }
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteCells(Connection connection, String nameSpace, String tableName, String rowKey) throws IOException {
        // 打开表
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        // 创建Delete操作对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 执行Delete操作
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭表
        table.close();
    }

    /**
     * 关闭HBase连接
     *
     * @param connection 需要关闭的HBase连接对象
     */
    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 向HBase表中写入一行数据
     *
     * @param conn
     * @param nameSpace
     * @param rowKey
     * @throws IOException
     */
    public static JSONObject putRow(Connection conn,
                                    String nameSpace,
                                    String tableName,
                                    String rowKey
    ) throws IOException {
        // 1. 获取 table 对象
        Table table = conn.getTable(TableName.valueOf(nameSpace, tableName));

        // 2. 创建 put 对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //3.调用get方法
        Result result = table.get(get);
        JSONObject jsonObj = new JSONObject();
        for (Cell cell : result.rawCells()) {
            jsonObj.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8), new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8));
        }


        table.close();
        return jsonObj;
    }


    public static JSONObject putAsyncRow(AsyncConnection conn,
                                    String nameSpace,
                                    String tableName,
                                    String rowKey
    ) throws IOException {
        // 1. 获取 table 对象
        AsyncTable table = conn.getTable(TableName.valueOf(nameSpace, tableName));

        // 2. 创建 put 对象
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject object = new JSONObject();

        //3.调用get方法
        try {
            Result result = (Result) table.get(get).get();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                object.put(Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return object ;
    }


}






