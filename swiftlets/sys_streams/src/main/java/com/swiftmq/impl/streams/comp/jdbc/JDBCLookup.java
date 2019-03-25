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

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.comp.memory.HeapMemory;
import com.swiftmq.impl.streams.comp.memory.Memory;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Facade to perform queries on a JDBC connected database.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class JDBCLookup {
    StreamContext ctx;
    String name;
    String driver;
    String url;
    String username;
    String password;
    String schema;
    Connection connection;
    Entity usage;
    int nCalls = 0;
    long callTime = 0;

    /**
     * Interval use.
     */
    public JDBCLookup(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        try {
            EntityList jdbcList = (EntityList) ctx.usage.getEntity("jdbclookups");
            usage = jdbcList.createEntity();
            usage.setName(name);
            usage.createCommands();
            jdbcList.addEntity(usage);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Sets the JDBC driver class name.
     *
     * @param driver class name
     * @return JDBCLookup
     */
    public JDBCLookup driver(String driver) {
        this.driver = driver;
        return this;
    }

    /**
     * Sets the JDBC URL.
     *
     * @param url JDBC URL
     * @return JDBCLookup
     */
    public JDBCLookup url(String url) {
        this.url = url;
        return this;
    }

    /**
     * Sets the database schema name.
     *
     * @param schema schema name
     * @return JDBCLoopkup
     */
    public JDBCLookup schema(String schema) {
        this.schema = schema;
        return this;
    }

    /**
     * Sets the JDBC username.
     *
     * @param username JDBC username
     * @return JDBCLookup
     */
    public JDBCLookup username(String username) {
        this.username = username;
        return this;
    }

    /**
     * Sets the JDBC password.
     *
     * @param password JDBC password
     * @return JDBCLookup
     */
    public JDBCLookup password(String password) {
        this.password = password;
        return this;
    }

    /**
     * Performs a connect to the database.
     *
     * @return JDBCLookup
     * @throws Exception if any error occurs
     */
    public JDBCLookup connect() throws Exception {
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        if (schema != null)
            connection.setSchema(schema);
        connection.setReadOnly(true);
        return this;
    }

    /**
     * Performs a select query and returns a Memory with the result. For every selected
     * database row a single Message is created. For each column of a row a message
     * property is created with the name of the column and the value as the corresponding
     * Java type (String, Integer, Long, Double, Float). A column which returns BigDecimal
     * as value type is converted into a Double.
     *
     * @param sql Select statement
     * @return Memory with the result
     * @throws Exception if any error occurs
     */
    public Memory query(String sql) throws Exception {
        if (connection == null || connection.isClosed())
            connect();
        nCalls++;
        long start = System.nanoTime();
        Memory resultMemory = new HeapMemory(ctx);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            Message message = ctx.messageBuilder.message();
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
        callTime += (System.nanoTime() - start) / 1000;
        return resultMemory;
    }

    /**
     * Internal use.
     */
    public void collect(long interval) {
        try {
            if (nCalls > 0)
                usage.getProperty("avg-query-time").setValue((int) (callTime / nCalls));
            else
                usage.getProperty("avg-query-time").setValue(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        nCalls = 0;
        callTime = 0;
    }

    /**
     * Closes the underlying JDBC connection. This is automatically called when the stream is stopped but
     * can also be called from a script if the JDBCLookup object isn't used anymore.
     */
    public void close() {
        try {
            ctx.usage.getEntity("jdbclookups").removeEntity(usage);
            if (!connection.isClosed())
                connection.close();
        } catch (Exception e) {

        }
        ctx.stream.removeJDBCLookup(name);
    }
}
