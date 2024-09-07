package com.xuqianvirtual.tracer.loader.simulator.tool;

import com.xuqianvirtual.tracer.common.model.ColumnDescriptor;
import com.xuqianvirtual.tracer.common.repository.OlapRepository;
import com.xuqianvirtual.tracer.common.util.GsonUtil;
import com.xuqianvirtual.tracer.loader.simulator.model.SimulatorDescriptor;
import com.xuqianvirtual.tracer.loader.simulator.model.TableSimulatorDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SimulatorTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimulatorTool.class);

  public static void main(String[] args) {
    try (InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(
        "simulator/meta_descriptor.yaml")) {
      Yaml yaml = new Yaml();
      SimulatorDescriptor simulatorDescriptor = yaml.loadAs(resourceAsStream, SimulatorDescriptor.class);
      LOGGER.info("meta descriptor is {}", GsonUtil.GSON.toJson(simulatorDescriptor));
      for (TableSimulatorDescriptor tableSimulatorDescriptor : simulatorDescriptor.getTableList()) {
        boolean result = dropTable(tableSimulatorDescriptor);
        if (!result) {
          LOGGER.warn("fail to drop OLAP table. [table_name = {}]", tableSimulatorDescriptor.getTableName());
        }
      }
      for (TableSimulatorDescriptor tableSimulatorDescriptor : simulatorDescriptor.getTableList()) {
        boolean result = createTable(tableSimulatorDescriptor);
        if (!result) {
          LOGGER.warn("fail to create OLAP table. [table = {}]", GsonUtil.GSON.toJson(tableSimulatorDescriptor));
        }
      }
    } catch (IOException e) {
      LOGGER.error("read simulator/meta_descriptor.yaml with exception.", e);
    }
  }

  private static boolean dropTable(TableSimulatorDescriptor tableDescriptor) {
    return OlapRepository.getInstance().dropTable(tableDescriptor.getTableName());
  }

  private static boolean createTable(TableSimulatorDescriptor tableDescriptor) {
    Map<String, Object> options = new HashMap<>();
    if (tableDescriptor.getProperties() != null) {
      options.put("properties", tableDescriptor.getProperties());
    }
    return OlapRepository.getInstance().createTable(tableDescriptor,
        tableDescriptor.getColumnDescriptors().stream().map(c -> (ColumnDescriptor) c).toList(),
        options);
  }
}
