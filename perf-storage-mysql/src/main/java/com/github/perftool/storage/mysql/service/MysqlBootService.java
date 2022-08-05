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

import com.github.perftool.storage.mysql.config.MysqlConfig;
import com.github.perftool.storage.mysql.module.OperationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class MysqlBootService {

    @Autowired
    private MysqlConfig mysqlConfig;

    public void boot() {
        this.initPerfTable();
        String[] operationTypes = mysqlConfig.operationType.split(",");
        for (int i = 0; i < operationTypes.length; i++) {
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(mysqlConfig.fixedThreadNum);
            fixedThreadPool.submit(new MysqlOperations(OperationType.valueOf(operationTypes[i]),
                    mysqlConfig.delayOperationSeconds, mysqlConfig));
        }
    }

    private void initPerfTable() {
        try (
                Connection conn = mysqlConfig.getDataSource().getConnection();
                Statement stmt = conn.createStatement();
        ) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + mysqlConfig.tableName + " (\n"
                    + "   id int primary key auto_increment,\n"
                    + "   stuno int not null unique,\n"
                    + "   stuname varchar(50) not null,\n"
                    + "   phone varchar(100),\n"
                    + "   idcard varchar(255),\n"
                    + "   addr varchar(255)\n"
                    + "   )");
        } catch (SQLException e) {
            log.error("create table fail. {}", mysqlConfig.tableName, e);
        }
    }

}
