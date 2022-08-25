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

package com.github.perftool.storage.cassandra.config;

import com.github.perftool.storage.common.config.QLCommonConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@Component
public class CassandraConfig extends QLCommonConfig {

    @Value("${CASSANDRA_CONNECT_TIMEOUT_SECONDS:30}")
    public int timeout;

    @Value("${CASSANDRA_USERNAME:}")
    public String username;

    @Value("${CASSANDRA_PASSWORD:}")
    public String password;

    @Value("${CASSANDRA_STRATEGY_NAME:SimpleStrategy}")
    public String strategyName;

    @Value("${CASSANDRA_REPLICATION_FACTOR:1}")
    public int replicationFactor;

    @Value("${CASSANDRA_DURABLE_WRITES:true}")
    public boolean durableWrites;

    @Value("${CASSANDRA_INDEX_ENABLE:false}")
    public boolean indexEnable;

    @Value("${CASSANDRA_COLUMN_NUM:10}")
    public int columnNum;

    @Value("${CASSANDRA_KEYSPACE_NUM:3}")
    public int keyspaceNum;

    @Value("${CASSANDRA_CONTACT_POINT_ADDRESS:localhost}")
    public String contactPointAddress;

    @Value("${CASSANDRA_PORT:9042}")
    public int port;

    @Value("${CASSANDRA_TABLE_NUM:3}")
    public int tableNum;
}
