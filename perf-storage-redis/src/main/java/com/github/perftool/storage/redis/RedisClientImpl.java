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

package com.github.perftool.storage.redis;

import com.github.perftool.storage.redis.config.RedisConfig;
import com.github.perftool.storage.redis.functional.SyncCommandCallback;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


import java.time.Duration;

@Slf4j
public class RedisClientImpl {

    private RedisConfig redisConfig;

    private GenericObjectPool<StatefulRedisConnection<String, String>> redisConnectionPool;

    public RedisClientImpl(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(redisConfig.maxActive);
        poolConfig.setMaxIdle(redisConfig.maxIdle);
        poolConfig.setMinIdle(redisConfig.minIdle);
        redisConnectionPool = ConnectionPoolSupport.createGenericObjectPool(() -> redisClient().connect(), poolConfig);
    }

    private RedisClient redisClient() {
        String[] url = redisConfig.clusterNodeUrl.split(":");
        RedisURI redisUrl = RedisURI.Builder
                .redis(url[0], Integer.parseInt(url[1]))
                .withDatabase(redisConfig.database)
                .withTimeout(Duration.ofSeconds(redisConfig.timeout))
                .withAuthentication(redisConfig.user, redisConfig.password.toCharArray())
                .build();
        return RedisClient.create(redisUrl);
    }

    private <T> T executeSync(SyncCommandCallback<T> callback) {
        try (StatefulRedisConnection<String, String> connection = redisConnectionPool.borrowObject()) {
            connection.setAutoFlushCommands(true);
            RedisCommands<String, String> commands = connection.sync();
            return callback.doInConnection(commands);
        } catch (Exception e) {
            log.warn("executeSync redis failed.", e);
            throw new RuntimeException(e);
        }
    }

    public String set(String key, String value) {
        return executeSync(commands -> commands.set(key, value));
    }

    public String get(String key) {
        return executeSync(commands -> commands.get(key));
    }

    public Long del(String... key) {
        return executeSync(commands -> commands.del(key));
    }

    public KeyScanCursor<String> scan(ScanArgs scanArgs) {
        return executeSync(commands -> commands.scan(scanArgs));
    }
}
