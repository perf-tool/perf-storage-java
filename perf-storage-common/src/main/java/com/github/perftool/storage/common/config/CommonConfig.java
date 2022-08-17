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

package com.github.perftool.storage.common.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@Component
public class CommonConfig {

    @Value("${DATA_SET_SIZE:100000}")
    public int dataSetSize;

    @Value("${FIXED_THREAD_NUM:10}")
    public int fixedThreadNum;

    @Value("${THREAD_RATE_LIMIT:1000}")
    public int threadRateLimit;

    @Value("${THREAD_RATE_LIMIT_TIMEOUT_MS:2}")
    public int threadRateLimitTimeoutMs;

    @Value("${READ_RATE_PERCENT:0.25}")
    public double readRatePercent;

    @Value("${UPDATE_RATE_PERCENT:0.75}")
    public double updateRatePercent;

}
