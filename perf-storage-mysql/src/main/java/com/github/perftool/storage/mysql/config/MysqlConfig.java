/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.storage.mysql.config;

import com.github.perftool.storage.common.config.QLCommonConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@Component
public class MysqlConfig extends QLCommonConfig {

    @Value("${MYSQL_HOST:localhost}")
    public String host;

    @Value("${MYSQL_PORT:3306}")
    public int port;

    @Value("${MYSQL_DB_NAME:}")
    public String dbName;

    @Value("${MYSQL_TABLE_COUNT:5}")
    public int tableCount;

    @Value("${MYSQL_USER:}")
    public String user;

    @Value("${MYSQL_PASSWORD:}")
    public String password;

    @Value("${MYSQL_MAXIMUM_POOL_SIZE:20}")
    public int maximumPoolSize;

}
