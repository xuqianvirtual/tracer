package com.xuqianvirtual.tracer.common.model;

public class ColumnDescriptor {
  // 列类型
  private DataType dataType;
  // 列名
  private String columnName;
  // 列描述
  private String columnComment;

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnComment() {
    return columnComment;
  }

  public void setColumnComment(String columnComment) {
    this.columnComment = columnComment;
  }
}
