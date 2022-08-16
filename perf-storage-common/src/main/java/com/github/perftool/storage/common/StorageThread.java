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

package com.github.perftool.storage.common;

import com.github.perftool.storage.common.config.CommonConfig;
import com.github.perftool.storage.common.utils.RandomUtils;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class StorageThread extends Thread {

    private final CommonConfig commonConfig;

    private final RateLimiter rateLimiter;

    public List<String> initIds;

    public StorageThread(CommonConfig commonConfig, List<String> initIds) {
        this.rateLimiter = RateLimiter.create(commonConfig.threadRateLimit);
        this.commonConfig = commonConfig;
        this.initIds = initIds;
    }

    @Override
    public void run() {
        while (true) {
            if (rateLimiter.tryAcquire(commonConfig.threadRateLimitTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    if (commonConfig.readRatePercent > RandomUtils.randomPercentage()) {
                        this.readData(initIds.get(RandomUtils.randomElem(initIds.size())));
                    }
                    if (commonConfig.updateRatePercent > RandomUtils.randomPercentage()) {
                        this.updateData(initIds.get(RandomUtils.randomElem(initIds.size())));
                    }
                } catch (Throwable e) {
                    log.error("unexpected exception ", e);
                }
            }
        }
    }

    public abstract void insertData(String id);

    public abstract void updateData(String id);

    public abstract void readData(String id);

    public abstract void deleteData(String id);

}
