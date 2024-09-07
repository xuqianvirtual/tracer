package com.xuqianvirtual.tracer.common.model;

import java.util.Map;

public class TableDescriptor {

  // 表类型
  private TableType tableType;
  // 表名
  private String tableName;
  // 表描述
  private String tableComment;
  // 时间分区字段
  private String rangeColumn;
  // 分区 hash 字段
  private String distributeColumn;
  // 分桶数量
  private Integer distributeBucket;
  // 属性信息
  private Map<String, Object> properties;

  public TableType getTableType() {
    return tableType;
  }

  public void setTableType(TableType tableType) {
    this.tableType = tableType;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableComment() {
    return tableComment;
  }

  public void setTableComment(String tableComment) {
    this.tableComment = tableComment;
  }

  public String getRangeColumn() {
    return rangeColumn;
  }

  public void setRangeColumn(String rangeColumn) {
    this.rangeColumn = rangeColumn;
  }

  public String getDistributeColumn() {
    return distributeColumn;
  }

  public void setDistributeColumn(String distributeColumn) {
    this.distributeColumn = distributeColumn;
  }

  public Integer getDistributeBucket() {
    return distributeBucket;
  }

  public void setDistributeBucket(Integer distributeBucket) {
    this.distributeBucket = distributeBucket;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }
}
