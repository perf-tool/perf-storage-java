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

package com.github.perftool.storage.mysql;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class IThread extends Thread {

    private final String operationType;
    private final int delaySeconds;
    protected final List<Integer> ids = new ArrayList<>();

    public IThread(String operationType, int delaySeconds) {
        setName("operation type - " + operationType);
        this.operationType = operationType;
        this.delaySeconds = delaySeconds;
    }

    @Override
    public void run() {
        while (true) {
            switch (operationType) {
                case "INSERT" -> insertData();
                case "UPDATE" -> updateData();
                case "READ" -> readData();
                case "DELETE" -> deleteData();
                default -> log.warn("an invalid operation type");
            }
            try {
                Thread.sleep(delaySeconds);
            } catch (InterruptedException e) {
                log.error("unexpected exception ", e);
            }
            if (ids.size() != 0) {
                deleteData();
            }
        }
    }

    public abstract void insertData();

    public abstract void updateData();

    public abstract void readData();

    public abstract void deleteData();
}
