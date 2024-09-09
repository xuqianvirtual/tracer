package com.xuqianvirtual.tracer.common.repository;

import com.xuqianvirtual.tracer.common.connector.DorisConnectors;
import com.xuqianvirtual.tracer.common.model.ColumnDescriptor;
import com.xuqianvirtual.tracer.common.model.TableDescriptor;
import com.xuqianvirtual.tracer.common.util.GsonUtil;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OlapRepository {

  private static final Logger LOGGER = LoggerFactory.getLogger(OlapRepository.class);

  private static volatile OlapRepository instance = null;

  private OlapRepository() {}

  public static OlapRepository getInstance() {
    if (instance == null) {
      synchronized (OlapRepository.class) {
        if (instance == null) {
          instance = new OlapRepository();
        }
      }
    }
    return instance;
  }

  public boolean dropTable(String tableName) {
    if (tableName == null || tableName.isBlank()) {
      LOGGER.warn("drop table name can not be blank. [table_name = {}]", tableName);
      return false;
    }
    String dropSql = "DROP TABLE " + tableName;
    try (Connection connection = DorisConnectors.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(dropSql)) {
      preparedStatement.execute();
      return true;
    } catch (SQLException | IOException e) {
      LOGGER.error("drop table with exception. [table = {}]", tableName, e);
      return false;
    }
  }

  public List<String> showTables() {
    List<String> tables = new ArrayList<>();
    try (Connection connection = DorisConnectors.getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("show tables");
      List<Map<String, Object>> mapList = new MapListHandler().handle(resultSet);
      for (Map<String, Object> map : mapList) {
        map.values().stream().findAny().ifPresent(table -> tables.add(table.toString()));
      }
    } catch (SQLException | IOException e) {
      LOGGER.error("show tables with exception.", e);
    }
    return tables;
  }

  public boolean createTable(TableDescriptor tableDescriptor, List<ColumnDescriptor> columnDescriptors,
      Map<String, Object> options) {
    if (tableDescriptor == null || columnDescriptors == null || columnDescriptors.isEmpty()) {
      LOGGER.warn("table and column descriptor must be valid. [table = {}, columns = {}]", tableDescriptor, columnDescriptors);
      return false;
    }
    if (options == null) {
      options = new HashMap<>();
    }
    StringBuilder createSqlBuilder = new StringBuilder();
    createSqlBuilder.append("CREATE TABLE ").append(tableDescriptor.getTableName()).append(" (");
    for (int i = 0; i < columnDescriptors.size(); i++) {
      createSqlBuilder.append(generateColumnCreateStatement(columnDescriptors.get(i)));
      if (i != columnDescriptors.size() - 1) {
        createSqlBuilder.append(",");
      }
    }
    createSqlBuilder.append(")");
    if (tableDescriptor.getTableComment() != null && !tableDescriptor.getTableComment().isBlank()) {
      createSqlBuilder.append(" COMMENT ? ");
    }
    if (tableDescriptor.getRangeColumn() != null && !tableDescriptor.getRangeColumn().isBlank()) {
      createSqlBuilder.append(" PARTITION BY RANGE(").append(tableDescriptor.getRangeColumn()).append(") () ");
    }
    StringBuilder distributeOptionBuilder = new StringBuilder();
    if (tableDescriptor.getDistributeColumn() != null && !tableDescriptor.getDistributeColumn().isBlank()) {
      distributeOptionBuilder.append(" HASH(").append(tableDescriptor.getDistributeColumn()).append(")");
    }
    if (tableDescriptor.getDistributeBucket() != null) {
      distributeOptionBuilder.append(" BUCKETS ").append(tableDescriptor.getDistributeBucket());
    }
    if (!distributeOptionBuilder.isEmpty()) {
      createSqlBuilder.append(" DISTRIBUTED BY ");
      createSqlBuilder.append(distributeOptionBuilder);
    }
    if (options.get("properties") != null) {
      createSqlBuilder.append(" PROPERTIES (");
      Map<String, Object> properties = (Map<String, Object>) options.get("properties");
      if (properties != null) {
        int i = 1;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
          createSqlBuilder.append("'").append(entry.getKey()).append("' = '").append(entry.getValue()).append("'");
          if (i != properties.size()) {
            createSqlBuilder.append(",");
          }
          i++;
        }
        createSqlBuilder.append(")");
      }
    }
    String createSqlTemplate = createSqlBuilder.toString();
    LOGGER.info("create OLAP table with sql. [sql = {}]", createSqlTemplate);
    try (Connection connection = DorisConnectors.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(createSqlTemplate)) {
      if (tableDescriptor.getTableComment() != null && !tableDescriptor.getTableComment().isBlank()) {
        preparedStatement.setString(1, tableDescriptor.getTableComment());
      }
      preparedStatement.execute();
      return true;
    } catch (SQLException | IOException throwables) {
      LOGGER.error("create table with exception. [table = {}, columns = {}]", GsonUtil.GSON.toJson(tableDescriptor), GsonUtil.GSON.toJson(columnDescriptors), throwables);
      return false;
    }
  }

  public void insertData(TableDescriptor tableDescriptor,
      List<ColumnDescriptor> columnDescriptors,
      List<Map<String, Object>> dataList) {
    if (tableDescriptor == null
        || columnDescriptors == null || columnDescriptors.isEmpty()
        || dataList == null || dataList.isEmpty()) {
      return;
    }
    StringBuilder insertSqlBuilder = new StringBuilder();
    insertSqlBuilder.append("INSERT INTO ").append(tableDescriptor.getTableName()).append(" (");
    for (int i = 0; i < columnDescriptors.size(); i++) {
      insertSqlBuilder.append(columnDescriptors.get(i).getColumnName());
      if (i != columnDescriptors.size() - 1) {
        insertSqlBuilder.append(",");
      }
    }
    insertSqlBuilder.append(") VALUES ");
    List<String> placeholderList = new ArrayList<>();
    for (int i = 0; i < columnDescriptors.size(); i++) {
      placeholderList.add("?");
    }
    for (int i = 0; i < dataList.size(); i++) {
      insertSqlBuilder.append("(").append(String.join(",", placeholderList)).append(")");
      if (i != dataList.size() - 1) {
        insertSqlBuilder.append(",");
      }
    }
    String insertSql = insertSqlBuilder.toString();
    LOGGER.info("insert OLAP table with sql. [sql = {}]", insertSql);
    try (Connection connection = DorisConnectors.getConnection();
        Statement statement = connection.createStatement();
        PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {
      statement.execute("set enable_insert_strict = false");
      int paramIndex = 1;
      for (Map<String, Object> map : dataList) {
        for (ColumnDescriptor columnDescriptor : columnDescriptors) {
          preparedStatement.setObject(paramIndex++, map.get(columnDescriptor.getColumnName()));
        }
      }

      preparedStatement.execute();
    } catch (SQLException | IOException throwables) {
      LOGGER.error("insert table with exception. [table = {}, columns = {}]", GsonUtil.GSON.toJson(tableDescriptor), GsonUtil.GSON.toJson(columnDescriptors), throwables);
    }
  }

  private String generateColumnCreateStatement(ColumnDescriptor columnDescriptor) {
    StringBuilder columnCreateBuilder = new StringBuilder();
    columnCreateBuilder.append(columnDescriptor.getColumnName()).append(" ");
    columnCreateBuilder.append(columnDescriptor.getDataType()).append(" ");
    if (columnDescriptor.getColumnComment() != null) {
      columnCreateBuilder.append(" COMMENT ").append("\"").append(columnDescriptor.getColumnComment()).append("\"");
    }
    return columnCreateBuilder.toString();
  }
}
