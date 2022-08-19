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

package com.github.perftool.storage.s3.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.perftool.storage.common.metrics.MetricFactory;
import com.github.perftool.storage.s3.config.S3Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class S3Service {

    @Autowired
    public S3Config s3Config;

    private AmazonS3 s3Client;

    public void initDatasource() {
        this.s3Client = createAmazonS3();
        if (!s3Client.doesBucketExistV2(s3Config.bucketName)) {
            s3Client.createBucket(s3Config.bucketName);
        }
    }

    public void presetData(MetricFactory metricFactory, List<String> keys) {
        S3StorageThread s3StorageThread = new S3StorageThread(s3Config, metricFactory, s3Client, keys);
        keys.forEach(s3StorageThread::insertData);
    }

    public void boot(MetricFactory metricFactory, List<String> keys) {
        for (int i = 0; i < s3Config.threadNum; i++) {
            new S3StorageThread(s3Config, metricFactory, s3Client, keys).start();
        }
    }

    private AmazonS3 createAmazonS3() {
        AWSCredentials credentials = new BasicAWSCredentials(s3Config.accessKey, s3Config.secretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(s3Config.awsConnectTimeoutMs);
        clientConfiguration.setRequestTimeout(s3Config.awsRequestTimeout);
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");
        return AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(s3Config.serviceEndpoint, Regions.US_EAST_1.name()))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }
}
