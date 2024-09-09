package com.xuqianvirtual.tracer.loader.simulator.model;

import com.xuqianvirtual.tracer.common.model.TableDescriptor;

import java.util.List;

public class TableSimulatorDescriptor extends TableDescriptor {

  // 生成数据的行数
  // 仅当 tableType 为 FACT_TABLE 时有效
  private int count;
  // 表包含的列信息
  private List<ColumnSimulatorDescriptor> columnDescriptors;

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public List<ColumnSimulatorDescriptor> getColumnDescriptors() {
    return columnDescriptors;
  }

  public void setColumnDescriptors(List<ColumnSimulatorDescriptor> columnDescriptors) {
    this.columnDescriptors = columnDescriptors;
  }
}
