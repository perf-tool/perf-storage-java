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

import com.github.perftool.storage.common.AbstractStorageThread;
import com.github.perftool.storage.common.metrics.MetricFactory;
import com.github.perftool.storage.common.module.OperationType;
import com.github.perftool.storage.common.utils.RandomUtils;
import com.github.perftool.storage.mysql.config.MysqlConfig;
import com.github.perftool.storage.mysql.constant.Constants;
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
public class MysqlStorageThread extends AbstractStorageThread {

    private final ConcurrentMap<String, String> cachedStatements = new ConcurrentHashMap<>();
    private final DefaultDBFlavor defaultDBFlavor;
    private final MysqlConfig mysqlConfig;
    private final DataSource dataSource;

    private final int tableIdx;

    public MysqlStorageThread(DataSource dataSource, MetricFactory metricFactory,
                              MysqlConfig mysqlConfig, List<String> ids, int tableIdx) {
        super(mysqlConfig, metricFactory, ids);
        this.defaultDBFlavor = new DefaultDBFlavor(mysqlConfig);
        this.mysqlConfig = mysqlConfig;
        this.dataSource = dataSource;
        this.tableIdx = tableIdx;
        for (int i = 0; i < mysqlConfig.tableCount; i++) {
            cachedStatements.putIfAbsent(OperationType.INSERT.name() + i,
                    defaultDBFlavor.insertStatement(Constants.DEFAULT_TABLE_NAME_PREFIX + i));
            cachedStatements.putIfAbsent(OperationType.UPDATE.name() + i,
                    defaultDBFlavor.updateStatement(Constants.DEFAULT_TABLE_NAME_PREFIX + i));
            cachedStatements.putIfAbsent(OperationType.READ.name() + i,
                    defaultDBFlavor.readStatement(Constants.DEFAULT_TABLE_NAME_PREFIX + i));
            cachedStatements.putIfAbsent(OperationType.DELETE.name() + i,
                    defaultDBFlavor.deleteStatement(Constants.DEFAULT_TABLE_NAME_PREFIX + i));
        }
    }

    @Override
    public void insertData(String id) {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements
                        .get(OperationType.INSERT.name() + tableIdx))
        ) {
            stmt.setString(1, id);
            for (int i = 2; i <= mysqlConfig.fieldCount; i++) {
                stmt.setString(i, RandomUtils.getRandomStr(mysqlConfig.fieldValueLength));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("insert data fail. ", e);
        }
    }

    @Override
    public void updateData(String id) {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements
                        .get(OperationType.UPDATE.name() + RandomUtils.randomElem(mysqlConfig.tableCount)))
        ) {
            for (int i = 1; i <= mysqlConfig.updateFieldCount; i++) {
                stmt.setString(i, RandomUtils.getRandomStr(mysqlConfig.fieldValueLength));
            }
            stmt.setString(mysqlConfig.updateFieldCount + 1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("update data fail. ", e);
        }
    }

    @Override
    public void readData(String id) {
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements
                        .get(OperationType.READ.name() + RandomUtils.randomElem(mysqlConfig.tableCount)))
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
                PreparedStatement stmt = conn.prepareStatement(cachedStatements
                        .get(OperationType.DELETE.name() + RandomUtils.randomElem(mysqlConfig.tableCount)))
        ) {
            if (initIds.size() == 0) {
                log.info("size is zero");
                return;
            }
            stmt.setString(1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("delete data fail. ", e);
        }
    }

}
