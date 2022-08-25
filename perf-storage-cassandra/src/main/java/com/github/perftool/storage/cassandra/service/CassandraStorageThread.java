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

package com.github.perftool.storage.cassandra.service;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.github.perftool.storage.cassandra.config.CassandraConfig;
import com.github.perftool.storage.cassandra.flavor.CassandraFlavor;
import com.github.perftool.storage.common.AbstractStorageThread;
import com.github.perftool.storage.common.metrics.MetricFactory;
import com.github.perftool.storage.common.module.OperationType;
import com.github.perftool.storage.common.utils.RandomUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class CassandraStorageThread extends AbstractStorageThread {

    private final Session session;
    private final int tableIdx;

    private final CassandraConfig cassandraConfig;

    private final ConcurrentMap<String, RegularStatement> cachedStatements = new ConcurrentHashMap<>();

    public CassandraStorageThread(CassandraConfig config, MetricFactory metricFactory,
                                  Session session, List<String> initIds, int tableIdx) {
        super(config, metricFactory, initIds);
        this.session = session;
        this.tableIdx = tableIdx;
        this.cassandraConfig = config;
        CassandraFlavor cassandraFlavor = new CassandraFlavor(config);
        for (int i = 0; i < config.tableNum; i++) {
            cachedStatements.putIfAbsent(OperationType.INSERT.name() + i,
                    (RegularStatement) new SimpleStatement(
                            cassandraFlavor.insertStatement(config.tableNamePrefix + i)
                    ).setConsistencyLevel(ConsistencyLevel.QUORUM));

            cachedStatements.putIfAbsent(OperationType.UPDATE.name() + i,
                    (RegularStatement) new SimpleStatement(
                            cassandraFlavor.updateStatement(config.tableNamePrefix + i)
                    ).setConsistencyLevel(ConsistencyLevel.QUORUM));

            cachedStatements.putIfAbsent(OperationType.READ.name() + i,
                    (RegularStatement) new SimpleStatement(
                            cassandraFlavor.readStatement(config.tableNamePrefix + i)
                    ).setConsistencyLevel(ConsistencyLevel.QUORUM));

            cachedStatements.putIfAbsent(OperationType.DELETE.name() + i,
                    (RegularStatement) new SimpleStatement(
                            cassandraFlavor.deleteStatement(config.tableNamePrefix + i)
                    ).setConsistencyLevel(ConsistencyLevel.QUORUM));
        }
    }

    @Override
    public void insertData(String id) {
        long start = System.currentTimeMillis();
        try {
            RegularStatement toPrepare = cachedStatements.get(OperationType.INSERT.name() + tableIdx);
            PreparedStatement prepared = session.prepare(toPrepare);
            Object[] param = new Object[cassandraConfig.fieldCount];
            param[0] = id;
            for (int i = 1; i < cassandraConfig.fieldCount; i++) {
                param[i] = RandomUtils.getRandomStr(cassandraConfig.fieldValueLength);
            }
            session.execute(prepared.bind(param));
            insertMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("insert cassandra data fail. ", e);
            insertMetricBean.fail(System.currentTimeMillis() - start);
        }
    }

    @Override
    public void updateData(String id) {
        long start = System.currentTimeMillis();
        try {
            RegularStatement toPrepare = cachedStatements.get(OperationType.UPDATE.name() + tableIdx);
            Object[] param = new Object[cassandraConfig.updateFieldCount + 1];
            for (int i = 0; i < cassandraConfig.updateFieldCount; i++) {
                param[i] = RandomUtils.getRandomStr(cassandraConfig.fieldValueLength);
            }
            param[cassandraConfig.updateFieldCount] = id;
            session.execute(session.prepare(toPrepare).bind(param));
            updateMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("update cassandra data fail. ", e);
            updateMetricBean.fail(System.currentTimeMillis() - start);
        }
    }

    @Override
    public void readData(String id) {
        long start = System.currentTimeMillis();
        try {
            RegularStatement toPrepare = cachedStatements.get(OperationType.READ.name() + tableIdx);
            session.execute(session.prepare(toPrepare).bind(id));
            readMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("read cassandra data fail. ", e);
            readMetricBean.fail(System.currentTimeMillis() - start);
        }
    }

    @Override
    public void deleteData(String id) {
        long start = System.currentTimeMillis();
        try {
            RegularStatement toPrepare = cachedStatements.get(OperationType.DELETE.name() + tableIdx);
            session.execute(session.prepare(toPrepare).bind(id));
            deleteMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            deleteMetricBean.fail(System.currentTimeMillis() - start);
        }
    }
}
