/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.storage.mysql.service;

import com.github.perftool.storage.common.utils.IDUtils;
import com.github.perftool.storage.mysql.config.MysqlConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class MysqlBootService {

    @Autowired
    private MysqlConfig mysqlConfig;

    public void boot() {
        List<String> ids = IDUtils.getTargetIds(mysqlConfig.dataSetSize);
        DataSource dataSource = createDatasource();
        this.initPerfTable(dataSource);
        this.initData(new MysqlOperations(dataSource, mysqlConfig, ids));
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(mysqlConfig.fixedThreadNum);
        for (int i = 0; i < mysqlConfig.fixedThreadNum; i++) {
            fixedThreadPool.execute(new MysqlOperations(dataSource, mysqlConfig, ids));
        }
    }

    public DataSource createDatasource() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
        String jdbcUrl = String.format("jdbc:mariadb://%s:%d/%s?user=%s&password=%s&allowPublicKeyRetrieval=true",
                mysqlConfig.host, mysqlConfig.port, mysqlConfig.dbName, mysqlConfig.user, mysqlConfig.password);
        hikariConfig.setJdbcUrl(jdbcUrl);
        return new HikariDataSource(hikariConfig);
    }

    private void initPerfTable(DataSource dataSource) {
        try (
                Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
        ) {
            StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + mysqlConfig.tableName + " ( ");
            sql.append("id varchar(" + mysqlConfig.fieldLength + ") primary key , ");
            for (int i = 1; i < mysqlConfig.fieldCount - 1; i++) {
                sql.append("field" + i + " varchar(" + mysqlConfig.fieldLength + "),");
            }
            sql.append("field" + (mysqlConfig.fieldCount - 1) + " varchar(" + mysqlConfig.fieldLength + ") ");
            sql.append(" )");
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            log.error("create table fail. {}", mysqlConfig.tableName, e);
        }
    }

    private void initData(MysqlOperations mysqlOperations) {
        mysqlOperations.ids.forEach(mysqlOperations::insertData);
    }

}
