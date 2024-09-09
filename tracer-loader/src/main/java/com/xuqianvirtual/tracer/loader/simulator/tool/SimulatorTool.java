package com.xuqianvirtual.tracer.loader.simulator.tool;

import com.xuqianvirtual.tracer.common.model.ColumnDescriptor;
import com.xuqianvirtual.tracer.common.model.DataType;
import com.xuqianvirtual.tracer.common.model.TableType;
import com.xuqianvirtual.tracer.common.repository.OlapRepository;
import com.xuqianvirtual.tracer.common.util.GsonUtil;
import com.xuqianvirtual.tracer.common.util.RandomChineseNameGenerator;
import com.xuqianvirtual.tracer.loader.simulator.model.ColumnSimulatorDescriptor;
import com.xuqianvirtual.tracer.loader.simulator.model.SimulatorDescriptor;
import com.xuqianvirtual.tracer.loader.simulator.model.TableSimulatorDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

public class SimulatorTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimulatorTool.class);

  public static void main(String[] args) {
    try (InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(
        "simulator/meta_descriptor.yaml")) {
      Yaml yaml = new Yaml();
      SimulatorDescriptor simulatorDescriptor = yaml.loadAs(resourceAsStream, SimulatorDescriptor.class);
      LOGGER.info("meta descriptor is {}", GsonUtil.GSON.toJson(simulatorDescriptor));
      rebuildDorisTable(simulatorDescriptor);
      Map<String, Set<Object>> dimensionIdsMap = new HashMap<>();
      Map<String, String> dimensionIdKeyMap = new HashMap<>();
      for (TableSimulatorDescriptor tableSimulatorDescriptor : simulatorDescriptor.getTableList()) {
        if (TableType.FACT_TABLE.equals(tableSimulatorDescriptor.getTableType())) {
          List<Map<String, Object>> mapList = generateFactData(tableSimulatorDescriptor);
          for (ColumnSimulatorDescriptor columnDescriptor : tableSimulatorDescriptor.getColumnDescriptors()) {
            if (columnDescriptor.getRelatedColumn() != null && !columnDescriptor.getRelatedColumn().isBlank()) {
              String[] tableColumn = columnDescriptor.getRelatedColumn().split("\\.");
              dimensionIdKeyMap.put(tableColumn[0], tableColumn[1]);
              Set<Object> ids = new HashSet<>();
              for (Map<String, Object> map : mapList) {
                ids.add(map.get(columnDescriptor.getColumnName()));
              }
              dimensionIdsMap.put(tableColumn[0], ids);
            }
          }
          insertTableData(tableSimulatorDescriptor, mapList);
        }
      }

      for (TableSimulatorDescriptor tableSimulatorDescriptor : simulatorDescriptor.getTableList()) {
        if (TableType.DIMENSION_TABLE.equals(tableSimulatorDescriptor.getTableType())) {
          Set<Object> ids = dimensionIdsMap.get(tableSimulatorDescriptor.getTableName());
          String idKey = dimensionIdKeyMap.get(tableSimulatorDescriptor.getTableName());
          List<Map<String, Object>> mapList = generateDimensionData(tableSimulatorDescriptor, idKey, ids);
          insertTableData(tableSimulatorDescriptor, mapList);
        }
      }
    } catch (IOException e) {
      LOGGER.error("read simulator/meta_descriptor.yaml with exception.", e);
    } catch (ParseException e) {
      LOGGER.error("generate fact table data with exception.", e);
    }
  }

  private static void insertTableData(TableSimulatorDescriptor tableDescriptor, List<Map<String, Object>> dataList) {
    OlapRepository.getInstance().insertData(
        tableDescriptor,
        tableDescriptor.getColumnDescriptors().stream().map(c -> (ColumnDescriptor) c).toList(),
        dataList);
  }

  private static void rebuildDorisTable(SimulatorDescriptor simulatorDescriptor) {
    List<String> tables = OlapRepository.getInstance().showTables();
    for (TableSimulatorDescriptor tableSimulatorDescriptor : simulatorDescriptor.getTableList()) {
      if (tables.contains(tableSimulatorDescriptor.getTableName())) {
        boolean result = dropTable(tableSimulatorDescriptor);
        if (!result) {
          LOGGER.warn("fail to drop OLAP table. [table_name = {}]", tableSimulatorDescriptor.getTableName());
        }
      }
    }
    for (TableSimulatorDescriptor tableSimulatorDescriptor : simulatorDescriptor.getTableList()) {
      boolean result = createTable(tableSimulatorDescriptor);
      if (!result) {
        LOGGER.warn("fail to create OLAP table. [table = {}]", GsonUtil.GSON.toJson(tableSimulatorDescriptor));
      }
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

  private static List<Map<String, Object>> generateDimensionData(TableSimulatorDescriptor tableDescriptor,
      String idKey,
      Set<Object> ids) throws ParseException {
    List<Map<String, Object>> dataList = new ArrayList<>();
    if (ids == null || ids.isEmpty()
        || tableDescriptor == null
        || tableDescriptor.getColumnDescriptors() == null
        || tableDescriptor.getColumnDescriptors().isEmpty()) {
      return dataList;
    }
    if (idKey == null || idKey.isBlank()) {
      return dataList;
    }
    if (tableDescriptor.getColumnDescriptors().stream().noneMatch(column -> column != null && Objects.equals(column.getColumnName(), idKey))) {
      return dataList;
    }
    for (Object id : ids) {
      Map<String, Object> lineData = new HashMap<>();
      for (ColumnSimulatorDescriptor columnDescriptor : tableDescriptor.getColumnDescriptors()) {
        if (Objects.equals(columnDescriptor.getColumnName(), idKey)) {
          lineData.put(columnDescriptor.getColumnName(), id);
        } else {
          lineData.put(columnDescriptor.getColumnName(), generateData(columnDescriptor));
        }
      }
      dataList.add(lineData);
    }

    return dataList;
  }

  private static List<Map<String, Object>> generateFactData(TableSimulatorDescriptor factTableDescriptor)
      throws ParseException {
    List<Map<String, Object>> factDataList = new ArrayList<>();
    if (factTableDescriptor == null
        || factTableDescriptor.getCount() < 1
        || factTableDescriptor.getColumnDescriptors() == null
        || factTableDescriptor.getColumnDescriptors().isEmpty()) {
      return factDataList;
    }
    List<ColumnSimulatorDescriptor> columnDescriptors = factTableDescriptor.getColumnDescriptors();
    for (int i = 0; i < factTableDescriptor.getCount(); i++) {
      Map<String, Object> lineData = new HashMap<>();
      for (ColumnSimulatorDescriptor columnDescriptor : columnDescriptors) {
        lineData.put(columnDescriptor.getColumnName(), generateData(columnDescriptor));
      }
      factDataList.add(lineData);
    }
    return factDataList;
  }

  private static Object generateData(ColumnSimulatorDescriptor columnDescriptor) throws ParseException {
    if (columnDescriptor == null) {
      return null;
    }
    DataType dataType = columnDescriptor.getDataType();
    List<Object> dataRange = columnDescriptor.getDataRange();
    Object value = null;
    switch (dataType) {
      case BIGINT -> value = new Random().nextInt(((Number) dataRange.get(0)).intValue(), ((Number) dataRange.get(1)).intValue());
      case DOUBLE -> value = new Random().nextDouble(((Number) dataRange.get(0)).doubleValue(), ((Number) dataRange.get(1)).doubleValue());
      case STRING -> {
        Object range0 = dataRange.get(0);
        if (Objects.equals("$$random_chinese_name$$", range0)) {
          value = RandomChineseNameGenerator.generateRandomChineseName(3);
        } else {
          value = dataRange.get(new Random().nextInt(dataRange.size()));
        }
      }
      case BOOLEAN -> value = new Random().nextBoolean();
      case DATETIME -> {
        Date fromDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataRange.get(0).toString());
        Date toDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataRange.get(1).toString());
        value = new Date(new Random().nextLong(fromDate.getTime(), toDate.getTime()));
      }
    }
    return value;
  }
}
