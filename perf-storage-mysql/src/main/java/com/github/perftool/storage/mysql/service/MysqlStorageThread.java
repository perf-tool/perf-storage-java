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
import com.github.perftool.storage.mysql.flavor.MysqlFlavor;
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
    private final MysqlFlavor mysqlFlavor;
    private final MysqlConfig mysqlConfig;
    private final DataSource dataSource;

    private final int tableIdx;

    public MysqlStorageThread(DataSource dataSource, MetricFactory metricFactory,
                              MysqlConfig mysqlConfig, List<String> ids, int tableIdx) {
        super(mysqlConfig, metricFactory, ids);
        this.mysqlFlavor = new MysqlFlavor(mysqlConfig);
        this.mysqlConfig = mysqlConfig;
        this.dataSource = dataSource;
        this.tableIdx = tableIdx;
        for (int i = 0; i < mysqlConfig.tableCount; i++) {
            cachedStatements.putIfAbsent(OperationType.INSERT.name() + i,
                    mysqlFlavor.insertStatement(mysqlConfig.tableNamePrefix + i));
            cachedStatements.putIfAbsent(OperationType.UPDATE.name() + i,
                    mysqlFlavor.updateStatement(mysqlConfig.tableNamePrefix + i));
            cachedStatements.putIfAbsent(OperationType.READ.name() + i,
                    mysqlFlavor.readStatement(mysqlConfig.tableNamePrefix + i));
            cachedStatements.putIfAbsent(OperationType.DELETE.name() + i,
                    mysqlFlavor.deleteStatement(mysqlConfig.tableNamePrefix + i));
        }
    }

    @Override
    public void insertData(String id) {
        long start = System.currentTimeMillis();
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
            insertMetricBean.success(System.currentTimeMillis() - start);
        } catch (SQLException e) {
            insertMetricBean.fail(System.currentTimeMillis() - start);
            log.error("mysql insert data fail. ", e);
        }
    }

    @Override
    public void updateData(String id) {
        long start = System.currentTimeMillis();
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
            updateMetricBean.success(System.currentTimeMillis() - start);
        } catch (SQLException e) {
            updateMetricBean.fail(System.currentTimeMillis() - start);
            log.error("mysql update data fail. ", e);
        }
    }

    @Override
    public void readData(String id) {
        long start = System.currentTimeMillis();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(cachedStatements
                        .get(OperationType.READ.name() + RandomUtils.randomElem(mysqlConfig.tableCount)))
        ) {
            stmt.setString(1, id);
            stmt.execute();
            readMetricBean.success(System.currentTimeMillis() - start);
        } catch (SQLException e) {
            readMetricBean.fail(System.currentTimeMillis() - start);
            log.error("mysql read data fail. ", e);
        }
    }

    @Override
    public void deleteData(String id) {
        long start = System.currentTimeMillis();
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
            deleteMetricBean.success(System.currentTimeMillis() - start);
        } catch (SQLException e) {
            deleteMetricBean.fail(System.currentTimeMillis() - start);
            log.error("delete data fail. ", e);
        }
    }

}
