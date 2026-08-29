/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * CloudSQLMYSQL database operations client.
 */
public class CloudMySqlClient {

    private static final Logger logger = Logger.getLogger(CloudMySqlClient.class);
    static Connection connection = null;

    private static Connection getCloudMySqlConnection() {
        if (connection == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl("jdbc:mysql:/" + E2ETestUtils.pluginProp("clsDatabaseName"));
            config.setUsername(System.getenv("Cloud_Mysql_User_Name"));
            config.setPassword(System.getenv("Cloud_Mysql_Password"));
            config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
            config.addDataSourceProperty("cloudSqlInstance", E2ETestUtils.pluginProp("cloudSqlInstance"));
            config.addDataSourceProperty("ipTypes", "PRIVATE");
            config.setMaximumPoolSize(5);
            config.setMinimumIdle(5);
            config.setConnectionTimeout(10000);
            config.setIdleTimeout(600000);
            config.setMaxLifetime(1800000);
            HikariDataSource pool =  new HikariDataSource(config);
            try {
                connection = pool.getConnection();
            } catch (SQLException e) {
                logger.error("Error while creating CloudSqlMySql connection : " + e);
            }
        }
        return connection;
    }

    public static Optional<String> getSoleQueryResult(String query) {
        String outputRowValue = null;
        try (PreparedStatement createTableStatement = getCloudMySqlConnection().prepareStatement(query);) {
            ResultSet resultSet = createTableStatement.executeQuery();
            if (resultSet.next()) {
                outputRowValue = resultSet.getString(1);
            }
        } catch (SQLException e) {
            logger.error("Error while executing CloudSqlMySql query : " + e);
        }
        return Optional.ofNullable(outputRowValue);
    }

    public static int countCloudSqlMySqlQuery(String table) {
        String query = "SELECT count(*) from " + table;
        return getSoleQueryResult(query).map(Integer::parseInt).orElse(0);
    }

    public static int getRecordsCount(String query) {
        int count = 0;
        try (PreparedStatement createTableStatement = getCloudMySqlConnection()
                .prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet resultSet = createTableStatement.executeQuery();
            if (resultSet.last()) {
                count =  resultSet.getRow();
            }
        } catch (SQLException e) {
            logger.error("Error while getting CloudSqlMySQL records count : " + e);
        }
        return count;
    }
}
