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

import com.github.perftool.storage.common.StorageThread;
import com.github.perftool.storage.common.utils.RandomUtils;
import com.github.perftool.storage.redis.config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;


@Slf4j
public class RedisOperations extends StorageThread {

    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisConfig redisConfig;
    private final List<String> ids;

    public RedisOperations(List<String> ids, RedisConfig redisConfig, RedisTemplate<String, Object> redisTemplate) {
        this.ids = ids;
        this.redisConfig = redisConfig;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run() {
        while (true) {
            this.updateData(ids.get(RandomUtils.randomElem(ids.size())));
            this.readData(ids.get(RandomUtils.randomElem(ids.size())));
        }
    }

    @Override
    public void insertData(String id) {
        redisTemplate.opsForValue().set(id, id);
    }

    @Override
    public void updateData(String id) {
        if (redisConfig.updateRatePercent < RandomUtils.randomPercentage()) {
            return;
        }
        log.info("the id {}", id);
        redisTemplate.opsForValue().set(id, id + "-update");

    }

    @Override
    public void readData(String id) {
        if (redisConfig.readRatePercent < RandomUtils.randomPercentage()) {
            return;
        }
        redisTemplate.opsForValue().get(id);
    }

    @Override
    public void deleteData(String id) {
        redisTemplate.delete(id);
    }
}
