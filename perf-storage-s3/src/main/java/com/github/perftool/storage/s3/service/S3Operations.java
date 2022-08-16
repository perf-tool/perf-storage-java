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
import com.github.perftool.storage.common.StorageThread;
import com.github.perftool.storage.common.utils.RandomUtils;
import com.github.perftool.storage.s3.config.S3Config;

import java.util.List;

public class S3Operations extends StorageThread {

    private final S3Config s3Config;
    private final AmazonS3 s3Client;

    public S3Operations(S3Config s3Config, AmazonS3 s3Client, List<String> keys) {
        super(s3Config, keys);
        this.s3Config = s3Config;
        this.s3Client = s3Client;
    }

    @Override
    public void insertData(String key) {
        s3Client.putObject(s3Config.bucketName, key, RandomUtils.getRandomStr(s3Config.fieldValueLength));
    }

    @Override
    public void updateData(String key) {
        s3Client.putObject(s3Config.bucketName, key, RandomUtils.getRandomStr(s3Config.fieldValueLength));
    }

    @Override
    public void readData(String key) {
        s3Client.getObject(s3Config.bucketName, key);
    }

    @Override
    public void deleteData(String key) {
        s3Client.deleteObject(s3Config.bucketName, key);
    }
}
