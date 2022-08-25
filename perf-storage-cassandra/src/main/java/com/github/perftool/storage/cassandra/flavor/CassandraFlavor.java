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

package com.github.perftool.storage.cassandra.flavor;

import com.github.perftool.storage.cassandra.config.CassandraConfig;
import com.github.perftool.storage.common.flavor.DefaultDBFlavor;

public class CassandraFlavor extends DefaultDBFlavor {

    private final CassandraConfig cassandraConfig;
    private static final String FIELD = "field";

    public CassandraFlavor(CassandraConfig cassandraConfig) {
        super(cassandraConfig);
        this.cassandraConfig = cassandraConfig;

    }

    @Override
    public String createDBStatement(String keyspace) {
        return String.format("CREATE KEYSPACE IF NOT EXISTS %s "
                        + "WITH replication = {'class': '%s', 'replication_factor' : '%s'} "
                        + "AND DURABLE_WRITES = %s", keyspace, cassandraConfig.strategyName,
                cassandraConfig.replicationFactor, cassandraConfig.durableWrites);
    }

    @Override
    public String createTableStatement(String tableName) {
        StringBuilder tableCql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        tableCql.append(tableName).append("( ");
        tableCql.append("id text PRIMARY KEY, ");
        for (int i = 1; i < cassandraConfig.columnNum - 1; i++) {
            tableCql.append(FIELD + i).append(" text, ");
        }
        tableCql.append(FIELD + (cassandraConfig.columnNum - 1)).append(" text )");
        if (cassandraConfig.indexEnable) {
            // TODO 创建表时，指定索引
        }
        return tableCql.toString();
    }
}
