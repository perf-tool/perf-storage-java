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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.github.perftool.storage.common.AbstractStorageThread;
import com.github.perftool.storage.common.metrics.MetricFactory;
import com.github.perftool.storage.common.utils.RandomUtils;
import com.github.perftool.storage.s3.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class S3StorageThread extends AbstractStorageThread {

    private final S3Config s3Config;
    private final AmazonS3 s3Client;

    public S3StorageThread(S3Config s3Config, MetricFactory metricFactory, AmazonS3 s3Client, List<String> keys) {
        super(s3Config, metricFactory, keys);
        this.s3Config = s3Config;
        this.s3Client = s3Client;
    }

    @Override
    public void insertData(String key) {
        long start = System.currentTimeMillis();
        try {
            s3Client.putObject(s3Config.bucketName, key, RandomUtils.getRandomStr(s3Config.dataSize));
            insertMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            insertMetricBean.fail(System.currentTimeMillis() - start);
            log.error("s3 put object error ", e);
        }
    }

    @Override
    public void updateData(String key) {
        long start = System.currentTimeMillis();
        try {
            s3Client.putObject(s3Config.bucketName, key, RandomUtils.getRandomStr(s3Config.dataSize));
            updateMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            updateMetricBean.fail(System.currentTimeMillis() - start);
            log.error("s3 update object error ", e);
        }
    }

    @Override
    public void readData(String key) {
        long start = System.currentTimeMillis();
        try {
            S3Object s3ClientObject = s3Client.getObject(s3Config.bucketName, key);
            IOUtils.toString(s3ClientObject.getObjectContent(), StandardCharsets.UTF_8);
            readMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            readMetricBean.fail(System.currentTimeMillis() - start);
            log.warn("s3 read content error ", e);
        }
    }

    @Override
    public void deleteData(String key) {
        long start = System.currentTimeMillis();
        try {
            s3Client.deleteObject(s3Config.bucketName, key);
            deleteMetricBean.success(System.currentTimeMillis() - start);
        } catch (Exception e) {
            deleteMetricBean.fail(System.currentTimeMillis() - start);
            log.warn("s3 delete content error ", e);
        }
    }
}
