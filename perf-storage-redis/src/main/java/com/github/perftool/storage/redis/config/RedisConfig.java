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

package com.github.perftool.storage.redis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@Component
public class RedisConfig {
    @Value("${REDIS_DATABASE:0}")
    public int database;

    @Value("${REDIS_HOST:192.168.122.131}")
    public String host;

    @Value("${REDIS_PASSWORD:}")
    public String password;

    @Value("${REDIS_PORT:6379}")
    public int port;

    @Value("${REDIS_TIMEOUT:5000}")
    public long timeout;

    @Value("${LETTUCE_SHUTDOWN_TIMEOUT_SECONDS:100}")
    public long shutDownTimeout;

    @Value("${LETTUCE_POOL_MAX_IDLE:0}")
    public int maxIdle;

    @Value("${LETTUCE_POOL_MIN_IDLE:5}")
    public int minIdle;

    @Value("${LETTUCE_POOL_MAX_ACTIVE:-1}")
    public int maxActive;

    @Value("${LETTUCE_POOL_MAX_WAIT:-1}")
    public long maxWait;

    @Value("${DELAY_OPERATION_SECONDS:0}")
    public int delayOperationSeconds;

    @Value("${OPERATION_TYPE:INSERT}")
    public String operationType;

}