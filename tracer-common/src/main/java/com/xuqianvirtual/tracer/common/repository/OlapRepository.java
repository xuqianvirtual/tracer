package com.xuqianvirtual.tracer.common.repository;

import com.xuqianvirtual.tracer.common.connector.DorisConnectors;
import com.xuqianvirtual.tracer.common.model.ColumnDescriptor;
import com.xuqianvirtual.tracer.common.model.TableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
      LOGGER.error("create table with exception. [table = {}, columns = {}]", tableDescriptor, columnDescriptors, throwables);
      return false;
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
