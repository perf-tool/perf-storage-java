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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.perftool.storage.cassandra.config.CassandraConfig;
import com.github.perftool.storage.cassandra.flavor.CassandraFlavor;
import com.github.perftool.storage.common.metrics.MetricFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;

@Service
@Slf4j
public class CassandraBootService {

    @Autowired
    private CassandraConfig cassandraConfig;

    public static final String KEY_SPACE = "perf_keyspace";
    private Cluster cluster;

    public void boot(MetricFactory metricFactory, List<String> keys) {
        int keyspaceNum = cassandraConfig.keyspaceNum - 1;
        while (keyspaceNum >= 0) {
            for (int i = 0; i < cassandraConfig.tableNum; i++) {
                for (int j = 0; j < cassandraConfig.threadNum; j++) {
                    new CassandraStorageThread(cassandraConfig, metricFactory,
                            cluster.connect(KEY_SPACE + keyspaceNum), keys, i).start();
                }
                keyspaceNum--;
            }
        }

    }

    public Set<String> listKeys() {
        Set<String> keys = new HashSet<>();
        try {
            Session session = cluster.connect(KEY_SPACE + 0);
            ResultSet ret = session.execute(String.format("SELECT id FROM %s", cassandraConfig.tableNamePrefix + 0));
            Iterator<Row> rowIterator = ret.iterator();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                keys.add(row.get(0, String.class));
            }
        } catch (Exception e) {
            log.error("read cassandra id fail . ", e);
        }
        return keys;
    }

    public void initDatasource() {
        cluster = this.createCluster();
        try (
                Session session = cluster.connect();
        ) {
            CassandraFlavor cassandraFlavor = new CassandraFlavor(cassandraConfig);
            for (int i = 0; i < cassandraConfig.keyspaceNum; i++) {
                session.execute(cassandraFlavor.createDBStatement(KEY_SPACE + i));
                Session keySession = cluster.connect(KEY_SPACE + i);
                for (int j = 0; j < cassandraConfig.tableNum; j++) {
                    keySession.execute(
                            cassandraFlavor.createTableStatement(cassandraConfig.tableNamePrefix + j));
                }
            }
        } catch (Exception e) {
            log.error("creat keyspace fail. ", e);
        }
    }

    private Cluster createCluster() {
        return Cluster.builder()
                .addContactPoint(cassandraConfig.contactPointAddress)
                .withPort(cassandraConfig.port)
                .withCredentials(cassandraConfig.username, cassandraConfig.password)
                .build();
    }

    public void presetData(MetricFactory metricFactory, List<String> keys) {
        for (int j = 0; j < cassandraConfig.keyspaceNum; j++) {
            try (
                    Session session = cluster.connect(KEY_SPACE + j)
            ) {
                for (int i = 0; i < cassandraConfig.tableNum; i++) {
                    this.presetData(metricFactory, keys, session, i);
                }
            } catch (Exception e) {
                log.error("preset data fail. ", e);
            }
        }
    }

    public void presetData(MetricFactory metricFactory, List<String> keys, Session session, int idx) {
        try {
            ExecutorService threadPool = Executors.newFixedThreadPool(cassandraConfig.presetThreadNum);
            CassandraStorageThread cassandraStorageThread =
                    new CassandraStorageThread(cassandraConfig, metricFactory, session, keys, idx);
            List<Callable<Object>> callableList =
                    keys.stream().map(s -> Executors.callable(() -> cassandraStorageThread.insertData(s))).toList();
            threadPool.invokeAll(callableList);
        } catch (Exception e) {
            log.error("preset data fail. ", e);
        }
    }
}


