package com.xuqianvirtual.tracer.loader.simulator.model;

import com.xuqianvirtual.tracer.common.model.TableDescriptor;

import java.util.List;

public class TableSimulatorDescriptor extends TableDescriptor {
  // 表包含的列信息
  private List<ColumnSimulatorDescriptor> columnDescriptors;

  public List<ColumnSimulatorDescriptor> getColumnDescriptors() {
    return columnDescriptors;
  }

  public void setColumnDescriptors(List<ColumnSimulatorDescriptor> columnDescriptors) {
    this.columnDescriptors = columnDescriptors;
  }
}
