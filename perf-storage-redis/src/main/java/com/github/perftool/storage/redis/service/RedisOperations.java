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

import com.github.perftool.storage.common.IThread;
import com.github.perftool.storage.common.module.OperationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;
import java.util.UUID;

@Slf4j
public class RedisOperations extends IThread {

    private final RedisTemplate<String, Object> redisTemplate;
    private List<String> ids;

    public RedisOperations(OperationType operationType, int delaySeconds,
                           RedisTemplate<String, Object> redisTemplate, List<String> ids) {
        super(operationType, delaySeconds, null);
        this.redisTemplate = redisTemplate;
        ids = this.ids;
    }

    @Override
    public void insertData() {
        String id = UUID.randomUUID().toString().replaceAll("-", "");
        redisTemplate.opsForValue().set(id, id);
    }

    @Override
    public void updateData() {
        redisTemplate.opsForValue().set(ids.get(0), ids.get(0) + "-update");
    }

    @Override
    public void readData() {
        redisTemplate.opsForValue().get(ids.get(0));
    }

    @Override
    public void deleteData() {
        redisTemplate.delete(ids.get(0));
        ids.remove(0);
    }
}
