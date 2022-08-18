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
    public String insertStatement(String tableName) {
        StringBuilder insertSql = new StringBuilder("INSERT INTO ");
        insertSql.append(tableName);
        insertSql.append(" (id,");
        for (int i = 1; i < mysqlConfig.fieldCount - 1; i++) {
            insertSql.append("field" + i + ", ");
        }
        insertSql.append("field" + (mysqlConfig.fieldCount - 1) + ")");
        insertSql.append(" VALUES(?");
        for (int i = 1; i < mysqlConfig.fieldCount; i++) {
            insertSql.append(",?");
        }
        insertSql.append(")");
        return insertSql.toString();
    }

    @Override
    public String readStatement(String tableName) {
        return "SELECT * FROM " + tableName + " where id = ?";
    }

    @Override
    public String deleteStatement(String tableName) {
        StringBuilder deleteSql = new StringBuilder("DELETE FROM ");
        deleteSql.append(tableName);
        deleteSql.append(" WHERE ");
        deleteSql.append("id");
        deleteSql.append(" = ?");
        System.out.println(deleteSql);
        return deleteSql.toString();
    }

    @Override
    public String updateStatement(String tableName) {
        StringBuilder update = new StringBuilder("UPDATE ");
        update.append(tableName);
        update.append(" SET ");
        for (int i = 1; i < mysqlConfig.updateFieldCount; i++) {
            update.append("field" + i + " = ?, ");
        }
        update.append("field" + mysqlConfig.updateFieldCount + " = ?");
        update.append(" WHERE ");
        update.append("id");
        update.append(" = ?");
        return update.toString();
    }

}
