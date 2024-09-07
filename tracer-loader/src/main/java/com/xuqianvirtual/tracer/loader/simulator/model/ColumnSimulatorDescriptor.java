package com.xuqianvirtual.tracer.loader.simulator.model;

import com.xuqianvirtual.tracer.common.model.ColumnDescriptor;

import java.util.List;

public class ColumnSimulatorDescriptor extends ColumnDescriptor {
  // 关联哪个维度表列
  private String relatedColumn;
  // 数据范围
  private List<Object> dataRange;

  public String getRelatedColumn() {
    return relatedColumn;
  }

  public void setRelatedColumn(String relatedColumn) {
    this.relatedColumn = relatedColumn;
  }

  public List<Object> getDataRange() {
    return dataRange;
  }

  public void setDataRange(List<Object> dataRange) {
    this.dataRange = dataRange;
  }
}
