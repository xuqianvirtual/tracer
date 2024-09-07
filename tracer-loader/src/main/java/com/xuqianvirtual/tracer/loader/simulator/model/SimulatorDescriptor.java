package com.xuqianvirtual.tracer.loader.simulator.model;

import java.util.List;

public class SimulatorDescriptor {
  // 表列表
  private List<TableSimulatorDescriptor> tableList;

  public List<TableSimulatorDescriptor> getTableList() {
    return tableList;
  }

  public void setTableList(List<TableSimulatorDescriptor> tableList) {
    this.tableList = tableList;
  }
}
