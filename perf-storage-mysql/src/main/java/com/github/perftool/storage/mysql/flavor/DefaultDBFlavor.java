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

package com.github.perftool.storage.mysql.flavor;

import com.github.perftool.storage.mysql.config.MysqlConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultDBFlavor extends DBFlavor {

    private final MysqlConfig mysqlConfig;

    public DefaultDBFlavor(MysqlConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    @Override
    public String insertStatement() {
        String[] fieldKeys = mysqlConfig.fieldString.split(",");
        StringBuilder insertSql = new StringBuilder("INSERT INTO ");
        insertSql.append(mysqlConfig.tableName);
        insertSql.append(" (" + mysqlConfig.fieldString + ")");
        insertSql.append(" VALUES(?");
        for (int i = 0; i < fieldKeys.length - 1; i++) {
            insertSql.append(",?");
        }
        insertSql.append(")");
        return insertSql.toString();
    }

    @Override
    public String readStatement() {
        StringBuilder readSql = new StringBuilder("SELECT * FROM " + mysqlConfig.tableName);
        return readSql.toString();
    }

    @Override
    public String deleteStatement() {
        StringBuilder deleteSql = new StringBuilder("DELETE FROM ");
        deleteSql.append(mysqlConfig.tableName);
        deleteSql.append(" WHERE ");
        deleteSql.append("id");
        deleteSql.append(" = ?");
        System.out.println(deleteSql);
        return deleteSql.toString();
    }

    @Override
    public String updateStatement() {
        String[] fieldKeys = new String[2];
        fieldKeys[0] = "stuname";
        fieldKeys[1] = "phone";
        StringBuilder update = new StringBuilder("UPDATE ");
        update.append(mysqlConfig.tableName);
        update.append(" SET ");
        for (int i = 0; i < fieldKeys.length; i++) {
            update.append(fieldKeys[i]);
            update.append(" = ?");
            if (i < fieldKeys.length - 1) {
                update.append(", ");
            }
        }
        update.append(" WHERE ");
        update.append("id");
        update.append(" = ?");
        return update.toString();
    }

}
