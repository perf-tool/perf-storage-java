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

import com.github.perftool.storage.mysql.IThread;
import com.github.perftool.storage.mysql.config.MysqlConfig;
import com.github.perftool.storage.mysql.flavor.DefaultDBFlavor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@Slf4j
public class MysqlOperations extends IThread {


    private final DefaultDBFlavor defaultDBFlavor;
    private final ConcurrentMap<String, PreparedStatement> cachedStatements = new ConcurrentHashMap<>();

    public MysqlOperations(String operationType, int delaySeconds,
                           Connection conn, MysqlConfig mysqlConfig) throws SQLException {
        super(operationType, delaySeconds);
        this.defaultDBFlavor = new DefaultDBFlavor(mysqlConfig);
        cachedStatements.putIfAbsent("INSERT",
                conn.prepareStatement(defaultDBFlavor.createInsertStatement()));
        cachedStatements.putIfAbsent("UPDATE",
                conn.prepareStatement(defaultDBFlavor.createUpdateStatement()));
        cachedStatements.putIfAbsent("READ",
                conn.prepareStatement(defaultDBFlavor.createReadStatement()));
        cachedStatements.putIfAbsent("DELETE",
                conn.prepareStatement(defaultDBFlavor.createDeleteStatement()));
    }

    @Override
    public void insertData() {
        try {
            PreparedStatement stmt = cachedStatements.get("INSERT");
            Random random = new Random();
            int key = random.nextInt();
            stmt.setInt(1,key);
            stmt.setInt(2,random.nextInt());
            stmt.setString(3, "zs");
            stmt.setString(4, random.nextInt() + "");
            stmt.setString(5, random.nextInt() + "");
            stmt.setString(6, "addr");
            int result = stmt.executeUpdate();
            if (result == 1) {
                ids.add(key);
                log.info("insert success.");
            } else {
                log.error("insert fail.");
            }
        } catch (SQLException e) {
            log.error("insert data fail. ", e);
        }
    }

    @Override
    public void updateData() {
        try {
            PreparedStatement stmt = cachedStatements.get("UPDATE");
            stmt.setInt(1, ids.get(0));
            int ret = stmt.executeUpdate();
            if (ret == 1) {
                log.info("update success.");
            } else {
                log.error("update fail.");
            }
        } catch (SQLException e) {
            log.error("update data fail. ", e);
        }
    }

    @Override
    public void readData() {
        try {
            PreparedStatement stmt = cachedStatements.get("READ");
            ResultSet resultSet = stmt.executeQuery();
            int count = 0;
            while (resultSet.next()) {
                count ++;
            }
            log.info("read data size : {}", count);
        } catch (SQLException e) {
            log.error("read data fail. ", e);
        }
    }

    @Override
    public void deleteData() {
        try {
            PreparedStatement stmt = cachedStatements.get("DELETE");
            stmt.setInt(1, ids.get(0));
            int ret = stmt.executeUpdate();
            if (ret == 1) {
                ids.remove(0);
                log.info("delete success.");
            } else {
                log.error("delete fail.");
            }
        } catch (SQLException e) {
            log.error("delete data fail. ", e);
        }

    }
}
