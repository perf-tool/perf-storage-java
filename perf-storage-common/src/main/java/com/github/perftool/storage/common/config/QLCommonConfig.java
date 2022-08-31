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
public class QLCommonConfig extends CommonConfig{

    @Value("${FIELDS_COUNT:10}")
    public int fieldCount;

    @Value("${UPDATE_FIELDS_COUNT:1}")
    public int updateFieldCount;

    @Value("${FIELD_LENGTH:100}")
    public String fieldLength;

    @Value("${FIELD_VALUE_LENGTH:1024}")
    public int fieldValueLength;

    @Value("${TABLE_NAME_PREFIX:perf_table}")
    public String tableNamePrefix;
}
