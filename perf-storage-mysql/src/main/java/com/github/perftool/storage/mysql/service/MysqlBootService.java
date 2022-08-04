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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class MysqlBootService {

    private static final String URL = "jdbc:mariadb://%s:%d/%s";
    private static final String DRIVER = "org.mariadb.jdbc.Driver";

    @Autowired
    private MysqlConfig mysqlConfig;

    public void boot() {
        try {
            Class.forName(DRIVER);
            Connection conn = DriverManager.getConnection(
                    String.format(URL, mysqlConfig.host, mysqlConfig.port, mysqlConfig.dbName)
                    , mysqlConfig.user, mysqlConfig.password);
            this.initPerfTable(conn);
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(mysqlConfig.fixedThreadNum);
            fixedThreadPool.submit(new MysqlOperations(mysqlConfig.operationType
                    , mysqlConfig.delayOperationSeconds, conn, mysqlConfig));
        } catch (ClassNotFoundException e) {
            log.error("class not found. ", e);
        } catch (SQLException e) {
            log.error("connected to mysql fail. ", e);
        }
    }

    private void initPerfTable(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS " + mysqlConfig.tableName);
            stmt.execute("CREATE TABLE "+ mysqlConfig.tableName +" (\n" +
                    "   id int primary key auto_increment,\n" +
                    "   stuno int not null unique,\n" +
                    "   stuname varchar(50) not null,\n" +
                    "   phone varchar(100),\n" +
                    "   idcard varchar(255),\n" +
                    "   addr varchar(255)\n" +
                    "   )");
        } catch (SQLException e) {
            log.error("create table fail. {}", mysqlConfig.tableName, e);
            throw new RuntimeException(e);
        }
    }

}
