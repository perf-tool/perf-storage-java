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

import com.github.perftool.storage.common.AbstractStorageThread;
import com.github.perftool.storage.common.metrics.MetricFactory;
import com.github.perftool.storage.common.utils.RandomUtils;
import com.github.perftool.storage.redis.config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;


@Slf4j
public class RedisStorageThread extends AbstractStorageThread {

    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisConfig redisConfig;

    public RedisStorageThread(List<String> ids, MetricFactory metricFactory,
                              RedisConfig redisConfig, RedisTemplate<String, Object> redisTemplate) {
        super(redisConfig, metricFactory, ids);
        this.redisConfig = redisConfig;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void insertData(String id) {
        long start = System.currentTimeMillis();
        try {
            redisTemplate.opsForValue().set(id, RandomUtils.getRandomStr(redisConfig.dataSize));
            insertMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            insertMetricBean.fail(System.currentTimeMillis() - start);
            log.error("redis insert data error ", e);
        }
    }

    @Override
    public void updateData(String id) {
        long start = System.currentTimeMillis();
        try {
            redisTemplate.opsForValue().set(id, RandomUtils.getRandomStr(redisConfig.dataSize));
            updateMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            updateMetricBean.fail(System.currentTimeMillis() - start);
            log.error("redis update data error ", e);
        }

    }

    @Override
    public void readData(String id) {
        long start = System.currentTimeMillis();
        try {
            redisTemplate.opsForValue().get(id);
            readMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            readMetricBean.fail(System.currentTimeMillis() - start);
            log.error("redis read data error ", e);
        }
    }

    @Override
    public void deleteData(String id) {
        long start = System.currentTimeMillis();
        try {
            Boolean isOk = redisTemplate.delete(id);
            if (Boolean.TRUE.equals(isOk)) {
                deleteMetricBean.success(System.currentTimeMillis() - start);
            } else {
                deleteMetricBean.fail(System.currentTimeMillis() - start);
            }
        } catch (Exception e) {
            deleteMetricBean.fail(System.currentTimeMillis() - start);
            log.error("redis delete data error ", e);
        }

    }
}
