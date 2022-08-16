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

package com.github.perftool.storage.s3.config;

import com.github.perftool.storage.common.config.CommonConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@Configuration
public class S3Config extends CommonConfig {

    @Value("${AWS_ACCESS_KEY:}")
    public String accessKey;

    @Value("${AWS_SECRET_KEY:}")
    public String secretKey;

    @Value("${BUCKET_NAME:testbucket}")
    public String bucketName;

    @Value("${AWS_SERVICE_ENDPOINT:http://localhost:9000}")
    public String serviceEndpoint;

    @Value("${FIELD_VALUE_LENGTH}")
    public int fieldValueLength;

}


