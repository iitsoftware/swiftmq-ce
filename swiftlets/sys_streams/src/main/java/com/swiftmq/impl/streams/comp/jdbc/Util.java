/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.impl.streams.comp.jdbc;

import com.swiftmq.impl.streams.Stream;
import com.swiftmq.impl.streams.comp.memory.HeapMemory;
import com.swiftmq.impl.streams.comp.memory.Memory;
import com.swiftmq.impl.streams.comp.message.Message;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class Util {
    public static Memory query(Stream stream, Connection connection, String sql) throws Exception {
        Memory resultMemory = new HeapMemory(stream.getStreamCtx());
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            Message message = stream.create().message().message();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String name = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);
                if (value instanceof BigDecimal)
                    value = ((BigDecimal) value).doubleValue();
                message.property(name).set(value);
            }
            resultMemory.add(message);
        }
        resultSet.close();
        statement.close();
        return resultMemory;
    }
}
