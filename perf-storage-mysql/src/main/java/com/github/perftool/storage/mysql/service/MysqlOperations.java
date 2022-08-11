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

import com.github.perftool.storage.common.StorageThread;
import com.github.perftool.storage.common.module.OperationType;
import com.github.perftool.storage.common.utils.RandomUtils;
import com.github.perftool.storage.mysql.config.MysqlConfig;
import com.github.perftool.storage.mysql.flavor.DefaultDBFlavor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@Slf4j
public class MysqlOperations extends StorageThread {

    private final DefaultDBFlavor defaultDBFlavor;
    private final ConcurrentMap<OperationType, String> cachedStatements = new ConcurrentHashMap<>();
    private final DataSource dataSource;
    private MysqlConfig mysqlConfig;
    public List<String> ids;


    public MysqlOperations(DataSource dataSource, MysqlConfig mysqlConfig, List<String> ids) {
        this.dataSource = dataSource;
        this.mysqlConfig = mysqlConfig;
        this.defaultDBFlavor = new DefaultDBFlavor(mysqlConfig);
        this.ids = ids;
        cachedStatements.putIfAbsent(OperationType.INSERT, defaultDBFlavor.insertStatement());
        cachedStatements.putIfAbsent(OperationType.UPDATE, defaultDBFlavor.updateStatement());
        cachedStatements.putIfAbsent(OperationType.READ, defaultDBFlavor.readStatement());
        cachedStatements.putIfAbsent(OperationType.DELETE, defaultDBFlavor.deleteStatement());
    }

    @Override
    public void run() {
        while (true) {
            this.readData(ids.get(RandomUtils.randomElem(ids.size())));
            this.updateData(ids.get(RandomUtils.randomElem(ids.size())));
        }
    }


    @Override
    public void insertData(String id) {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.INSERT))
        ) {
            stmt.setString(1, id);
            for (int i = 2; i <= mysqlConfig.fieldCount; i++) {
                stmt.setString(i, RandomUtils.random() + "");
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
    public void updateData(String id) {
        if (RandomUtils.randomPercentage() > mysqlConfig.updateRatePercent) {
            return;
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.UPDATE))
        ) {
            for (int i = 1; i <= mysqlConfig.updateFieldCount; i++) {
                stmt.setString(i, RandomUtils.random() + "");
            }
            stmt.setString(mysqlConfig.updateFieldCount + 1, id);
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
    public void readData(String id) {
        if (mysqlConfig.readRatePercent < RandomUtils.randomPercentage()) {
            return;
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.READ))
        ) {
            stmt.setString(1, id);
            stmt.execute();
        } catch (SQLException e) {
            log.error("read data fail. ", e);
        }
    }

    @Override
    public void deleteData(String id) {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements.get(OperationType.DELETE))
        ) {
            if (ids.size() == 0) {
                log.info("size is zero");
                return;
            }
            stmt.setString(1, id);
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

}
