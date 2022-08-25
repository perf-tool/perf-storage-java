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

package com.github.perftool.storage.service;


import com.github.perftool.storage.cassandra.service.CassandraBootService;
import com.github.perftool.storage.common.config.CommonConfig;
import com.github.perftool.storage.common.config.StorageType;
import com.github.perftool.storage.common.metrics.MetricFactory;
import com.github.perftool.storage.common.service.MetricsService;
import com.github.perftool.storage.common.utils.IDUtils;
import com.github.perftool.storage.config.StorageConfig;
import com.github.perftool.storage.mysql.service.MysqlService;
import com.github.perftool.storage.redis.service.RedisService;
import com.github.perftool.storage.s3.service.S3Service;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class BootService {

    @Autowired
    private StorageConfig storageConfig;

    @Autowired
    private CommonConfig commonConfig;

    @Autowired
    private MysqlService mysqlService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private MetricsService metricsService;

    @Autowired
    private CassandraBootService cassandraBootService;

    @PostConstruct
    public void init() {
        log.info("storage type : {}", storageConfig.storageType);
        MetricFactory metricFactory = metricsService.acquireMetricFactory(storageConfig.storageType);
        ExecutorService executorService =
                Executors.newSingleThreadExecutor(new DefaultThreadFactory("perf-storage-init"));
        executorService.execute(() -> BootService.this.initAsync(metricFactory));
    }

    /**
     * use init async to let the springboot framework run
     */
    public void initAsync(MetricFactory metricFactory) {
        switch (storageConfig.storageType) {
            case DUMMY -> log.info("dummy storage");
            case MYSQL -> mysqlService.initDatasource();
            case REDIS -> redisService.initDatasource();
            case S3 -> s3Service.initDatasource();
            case CASSANDRA -> cassandraBootService.initDatasource();
            default -> {
            }
        }

        Set<String> nowKeys = new HashSet<>();
        switch (storageConfig.storageType) {
            case DUMMY -> log.info("dummy storage");
            case MYSQL -> nowKeys = mysqlService.listKeys();
            case REDIS -> nowKeys = redisService.listKeys();
            case S3 -> nowKeys = s3Service.listKeys();
            case CASSANDRA -> nowKeys = cassandraBootService.listKeys();
            default -> {
            }
        }
        log.info("the now key : {}", nowKeys);
        List<String> keys = new ArrayList<>();
        int needDataSetSize = commonConfig.dataSetSize - nowKeys.size();
        if (needDataSetSize > 0) {
            keys = IDUtils.getTargetIds(needDataSetSize);
            switch (storageConfig.storageType) {
                case DUMMY -> log.info("dummy storage");
                case MYSQL -> mysqlService.presetData(metricFactory, keys);
                case REDIS -> redisService.presetData(metricFactory, keys);
                case S3 -> s3Service.presetData(metricFactory, keys);
                case CASSANDRA -> cassandraBootService.presetData(metricFactory, keys);
                default -> {
                }
            }
        }
        keys.addAll(nowKeys);
        if (storageConfig.storageType == StorageType.DUMMY) {
            log.info("dummy storage");
        } else if (storageConfig.storageType == StorageType.MYSQL) {
            mysqlService.boot(metricFactory, keys);
        } else if (storageConfig.storageType == StorageType.REDIS) {
            redisService.boot(metricFactory, keys);
        } else if (storageConfig.storageType == StorageType.S3) {
            s3Service.boot(metricFactory, keys);
        } else if (storageConfig.storageType == StorageType.CASSANDRA) {
            cassandraBootService.boot(metricFactory, keys);
        }
    }

}

