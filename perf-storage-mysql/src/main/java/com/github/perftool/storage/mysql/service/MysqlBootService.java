/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.storage.mysql.service;

import com.github.perftool.storage.common.module.OperationType;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class MysqlBootService {

    @Autowired
    private MysqlConfig mysqlConfig;
    public List<Integer> ids = Collections.synchronizedList(new ArrayList<>());

    public void boot() {
        DataSource dataSource = createDatasource(mysqlConfig);
        this.initPerfTable(dataSource);
        String[] operationTypes = mysqlConfig.operationType.split(",");
        for (String operationType : operationTypes) {
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(mysqlConfig.fixedThreadNum);
            MysqlOperations mysqlOperations = new MysqlOperations(OperationType.valueOf(operationType),
                    mysqlConfig.delayOperationSeconds, dataSource, mysqlConfig, ids);
            for (int i = 0; i < mysqlConfig.fixedThreadNum; i++) {
                fixedThreadPool.submit(mysqlOperations);
            }
        }
    }

    public DataSource createDatasource(MysqlConfig conf) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
        String jdbcUrl = String.format("jdbc:mariadb://%s:%d/%s?user=%s&password=%s&allowPublicKeyRetrieval=true",
                conf.host, conf.port, conf.dbName, conf.user, conf.password);
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

}
