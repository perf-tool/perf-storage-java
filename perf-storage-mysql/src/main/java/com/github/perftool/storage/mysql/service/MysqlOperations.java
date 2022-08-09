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

import com.github.perftool.storage.common.IThread;
import com.github.perftool.storage.common.module.OperationType;
import com.github.perftool.storage.mysql.config.MysqlConfig;
import com.github.perftool.storage.mysql.flavor.DefaultDBFlavor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@Slf4j
public class MysqlOperations extends IThread {

    private final DefaultDBFlavor defaultDBFlavor;
    private final ConcurrentMap<OperationType, String> cachedStatements = new ConcurrentHashMap<>();
    private final DataSource dataSource;
    private final SecureRandom random = new SecureRandom();
    private final MysqlConfig mysqlConfig;
    private List<String> ids = Collections.synchronizedList(new ArrayList<>());

    public MysqlOperations(OperationType operationType, int delaySeconds,
                           DataSource dataSource, MysqlConfig mysqlConfig, List<Integer> ids) {
        super(operationType, delaySeconds);
        this.dataSource = dataSource;
        this.mysqlConfig = mysqlConfig;
        this.defaultDBFlavor = new DefaultDBFlavor(mysqlConfig);
        cachedStatements.putIfAbsent(OperationType.INSERT, defaultDBFlavor.insertStatement());
        cachedStatements.putIfAbsent(OperationType.UPDATE, defaultDBFlavor.updateStatement());
        cachedStatements.putIfAbsent(OperationType.READ, defaultDBFlavor.readStatement());
        cachedStatements.putIfAbsent(OperationType.DELETE, defaultDBFlavor.deleteStatement());
    }

    @Override
    public void insertData() {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.INSERT))
        ) {
            String id = UUID.randomUUID().toString().replaceAll("-", "");
            stmt.setString(1, id);
            for (int i = 2; i <= mysqlConfig.fieldCount; i++) {
                stmt.setString(i, getRandom() + "");
            }
            int result = stmt.executeUpdate();
            if (result == 1) {
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
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.UPDATE))
        ) {
            if (ids.size() == 0) {
                ids = readAllID();
                log.info("size is zero");
                return;
            }
            log.info("the ids : {}", ids);
            for (int i = 1; i <= mysqlConfig.updateFieldCount; i++) {
                stmt.setString(i, "update-" + getRandom());
            }
            stmt.setString(mysqlConfig.updateFieldCount + 1, ids.get(0));
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
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.READ))
        ) {
            ResultSet resultSet = stmt.executeQuery();
            int count = 0;
            while (resultSet.next()) {
                count++;
                String id = resultSet.getString("id");
                log.info("id : {} ", id);
            }
            log.info("read data size : {}", count);
        } catch (SQLException e) {
            log.error("read data fail. ", e);
        }
    }

    @Override
    public void deleteData() {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.DELETE))
        ) {
            if (ids.size() == 0) {
                ids = readAllID();
                log.info("size is zero");
                return;
            }
            stmt.setString(1, ids.get(0));
            int ret = stmt.executeUpdate();
            if (ret == 1) {
                log.info("delete success.");
                ids.remove(0);
            } else {
                log.error("delete fail.");
            }
        } catch (SQLException e) {
            log.error("delete data fail. ", e);
        }
    }

    private int getRandom() {
        return random.nextInt();
    }

    private List<String> readAllID() {
        List<String> ids = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.READ))
        ) {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                ids.add(resultSet.getString("id"));
            }
        } catch (SQLException e) {
            log.error("read data fail. ", e);
        }
        return ids;
    }
}
