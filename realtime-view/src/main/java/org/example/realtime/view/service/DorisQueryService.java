package org.example.realtime.view.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class DorisQueryService {
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    /**
     * 获取所有可用的DWS表
     */
    public List<String> getAvailableTables() {
        String sql = "SHOW TABLES";
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    /**
     * 获取表的基本信息
     */
    public TableInfo getTableInfo(String tableName) {
        String sql = "DESC " + tableName;
        List<Map<String, Object>> columns = jdbcTemplate.queryForList(sql);
        
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(tableName);
        tableInfo.setColumns(new ArrayList<>());
        
        for (Map<String, Object> column : columns) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setFieldName((String) column.get("Field"));
            columnInfo.setType((String) column.get("Type"));
            columnInfo.setNullValue((String) column.get("Null"));
            columnInfo.setKey((String) column.get("Key"));
            columnInfo.setDefaultValue(column.get("Default"));
            columnInfo.setExtra((String) column.get("Extra"));
            tableInfo.getColumns().add(columnInfo);
        }
        
        return tableInfo;
    }
    
    /**
     * 查询表数据（带分页）
     */
    public Map<String, Object> queryTableData(String tableName, int page, int size) {
        int offset = (page - 1) * size;
        
        // 获取总记录数
        String countSql = "SELECT COUNT(*) FROM " + tableName;
        Long total = jdbcTemplate.queryForObject(countSql, Long.class);
        
        // 获取分页数据
        String dataSql = "SELECT * FROM " + tableName + " LIMIT " + offset + ", " + size;
        List<Map<String, Object>> data = jdbcTemplate.queryForList(dataSql);
        
        Map<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("page", page);
        result.put("size", size);
        result.put("data", data);
        
        return result;
    }
    
    /**
     * 执行自定义SQL查询
     */
    public List<Map<String, Object>> executeQuery(String sql) {
        return jdbcTemplate.queryForList(sql);
    }

    /**
     * 执行SQL查询并返回单个对象
     */
    public <T> T executeQueryForObject(String sql, Class<T> requiredType) {
        try {
            return jdbcTemplate.queryForObject(sql, requiredType);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }
    

    
    @Data
    public static class TableInfo {
        private String tableName;
        private List<ColumnInfo> columns;
    }
    
    @Data
    public static class ColumnInfo {
        private String fieldName;
        private String type;
        private String nullValue;
        private String key;
        private Object defaultValue;
        private String extra;
    }
}