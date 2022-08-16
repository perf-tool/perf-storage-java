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

package com.github.perftool.storage.common.utils;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class RandomUtils {

    private static final Random random = new Random();

    public static double randomPercentage() {
        return random.nextDouble(1);
    }

    public static int random() {
        return random.nextInt();
    }

    public static int randomElem(int listSize) {
        return random.nextInt(listSize);
    }

    public static String getRandomStr(int strLength) {
        StringBuilder messageBuilder = new StringBuilder(strLength);
        for (int i = 0; i < strLength; i++) {
            messageBuilder.append('a' + ThreadLocalRandom.current().nextInt(26));
        }
        return messageBuilder.toString();
    }

    public static byte[] getRandomBytes(int messageByte) {
        byte[] array = new byte[messageByte];
        for (int i = 0; i < messageByte; i++) {
            array[i] = (byte) (97 + ThreadLocalRandom.current().nextInt(26));
        }
        return array;
    }

}
