package com.xuqianvirtual.tracer.common.connector;

import com.xuqianvirtual.tracer.common.util.GsonUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DorisConnectors {

  private static final Logger LOGGER = LoggerFactory.getLogger(DorisConnectors.class);

  private static volatile DorisConnectors instance = null;

  private HikariDataSource dataSource = null;

  private DorisConnectors() {}

  private static DorisConnectors getInstance() throws IOException {
    if (instance == null) {
      synchronized (DorisConnectors.class) {
        if (instance == null) {
          try (InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(
              "doris_config.yaml")) {
            Yaml yaml = new Yaml();
            HikariConfig dorisConfig = yaml.loadAs(resourceAsStream, HikariConfig.class);
            LOGGER.info("doris config is {}", GsonUtil.GSON.toJson(dorisConfig));
            instance = new DorisConnectors();
            instance.dataSource = new HikariDataSource(dorisConfig);
          }
        }
      }
    }
    return instance;
  }

  public static Connection getConnection() throws IOException, SQLException {
    return getInstance().dataSource.getConnection();
  }

  public static void main(String[] args) {
    try (Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("show databases");
        ResultSet resultSet = preparedStatement.executeQuery()) {
      MapListHandler mapListHandler = new MapListHandler();
      List<Map<String, Object>> mapList = mapListHandler.handle(resultSet);
      LOGGER.info("query result is {}", GsonUtil.GSON.toJson(mapList));
    } catch (SQLException | IOException throwables) {
      LOGGER.error("get doris connection with exception.", throwables);
    }
  }
}
