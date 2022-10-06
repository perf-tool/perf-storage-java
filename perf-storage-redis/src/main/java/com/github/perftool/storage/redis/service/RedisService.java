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

package com.github.perftool.storage.redis.service;

import com.github.perftool.storage.common.metrics.MetricFactory;
import com.github.perftool.storage.redis.RedisClientImpl;
import com.github.perftool.storage.redis.config.RedisConfig;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@Service
public class RedisService {
    private RedisConfig redisConfig;

    private RedisClientImpl redisClientImpl;

    public RedisService(@Autowired RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public void initDatasource() {
        this.redisClientImpl = new RedisClientImpl(redisConfig);
    }

    public Set<String> listKeys() {
        KeyScanCursor scan = redisClientImpl.scan(ScanArgs.Builder
                .limit(redisConfig.dataSetSize)
                .match("*"));
        return new HashSet<>(scan.getKeys());
    }

    public void presetData(MetricFactory metricFactory, List<String> keys) {
        ExecutorService threadPool = Executors.newFixedThreadPool(redisConfig.presetThreadNum);
        RedisStorageThread redisStorageThread =
                new RedisStorageThread(keys, metricFactory, redisConfig, redisClientImpl);
        List<Callable<Object>> callableList =
                keys.stream().map(s -> Executors.callable(() -> redisStorageThread.insertData(s)))
                        .collect(Collectors.toList());
        try {
            threadPool.invokeAll(callableList);
        } catch (InterruptedException e) {
            log.error("preset s3 data failed ", e);
        }
    }

    public void boot(MetricFactory metricFactory, List<String> keys) {
        for (int i = 0; i < redisConfig.threadNum; i++) {
            new RedisStorageThread(keys, metricFactory, redisConfig, redisClientImpl).start();
        }
    }
}
