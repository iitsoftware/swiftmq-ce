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

package com.swiftmq.impl.accounting.standard.factoryimpl;

import com.swiftmq.impl.accounting.standard.SwiftletContext;
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.accounting.AccountingSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class JDBCSink implements AccountingSink
{
  SwiftletContext ctx = null;
  String className = null;
  String url = null;
  String username = null;
  String password = null;
  String statement = null;
  List fieldMapping = null;
  List typeMapping = null;
  Connection connection = null;
  PreparedStatement pStatement = null;

  public JDBCSink(SwiftletContext ctx, String className, String url, String username, String password, String statement, List fieldMapping, List typeMapping)
  {
    this.ctx = ctx;
    this.className = className;
    this.url = url;
    this.username = username;
    this.password = password;
    this.statement = statement;
    this.fieldMapping = fieldMapping;
    this.typeMapping = typeMapping;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/created");
  }

  private void createConnection() throws Exception
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/createConnection ...");
    Class.forName(className);
    connection = DriverManager.getConnection(url, username, password);
    connection.setAutoCommit(true);
    pStatement = connection.prepareStatement(statement);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/createConnection done, connection=" + connection);
  }

  public void add(MapMessageImpl msg) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/add, msg=" + msg);
    if (connection == null)
      createConnection();
    for (int i = 0; i < fieldMapping.size(); i++)
    {
      String name = (String) fieldMapping.get(i);
      String value = msg.getString(name);
      if (value == null)
        throw new Exception("Field name '" + name + "' not found in accounting message!");
      String type = (String) typeMapping.get(i);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/add, name=" + name + ", value=" + value + ", type=" + type);
      type = type.toLowerCase();
      if (type.equals("int"))
        pStatement.setInt(i + 1, Integer.parseInt(value));
      else if (type.equals("long"))
        pStatement.setLong(i + 1, Long.parseLong(value));
      else if (type.equals("double"))
        pStatement.setDouble(i + 1, Double.parseDouble(value));
      else
        pStatement.setString(i + 1, value);
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/add, executeUpdate");
    pStatement.executeUpdate();
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/close");
    try
    {
      if (pStatement != null)
        pStatement.close();
    } catch (SQLException e)
    {
    }
    try
    {
      if (connection != null)
        connection.close();
    } catch (SQLException e)
    {
    }
    connection = null;
    pStatement = null;
  }

  public String toString()
  {
    return "JDBCSink, url=" + url;
  }
}
