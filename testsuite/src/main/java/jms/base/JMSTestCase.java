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

package jms.base;

import com.swiftmq.admin.cli.CLI;
import junit.framework.TestCase;

import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Hashtable;

public class JMSTestCase extends TestCase
{
  QueueConnection cliQC = null;
  private CLI cli = null;
  protected static int id = 0;

  public JMSTestCase(String name)
  {
    super(name);
  }

  public InitialContext createInitialContext(String className, String url)
  {
    InitialContext ctx = null;
    try
    {
      Hashtable env = new Hashtable();
      env.put(Context.INITIAL_CONTEXT_FACTORY, className);
      env.put(Context.PROVIDER_URL, url);
      ctx = new InitialContext(env);
    } catch (Exception e)
    {
      failFast("Failed to create InitialContext from class: " + className + ", url: " + url + ", exception=" + e);
    }
    return ctx;
  }

  public InitialContext createInitialContext()
  {
    String className = System.getProperty("jndi.class");
    assertNotNull("missing property 'jndi.class'", className);
    String url = System.getProperty("jndi.url");
    assertNotNull("missing property 'jndi.url'", url);
    return createInitialContext(className, url);
  }

  public void pause(long ms)
  {
    try
    {
      Thread.sleep(ms);
    } catch (Exception ignored)
    {
    }
  }

  private void createCLI() throws Exception
  {
    InitialContext ctx = createInitialContext();
    QueueConnectionFactory qcf = (QueueConnectionFactory) ctx.lookup("QueueConnectionFactory");
    cliQC = qcf.createQueueConnection();
    cli = new CLI(cliQC);
    ctx.close();
  }

  public void createQueue(String queueName) throws Exception
  {
    if (cli == null)
      createCLI();
    cli.waitForRouter("router");
    cli.executeCommand("sr router");
    cli.executeCommand("cc /sys$queuemanager/queues");
    cli.executeCommand("new " + queueName);
  }

  public void createTopic(String topicName) throws Exception
  {
    if (cli == null)
      createCLI();
    cli.waitForRouter("router");
    cli.executeCommand("sr router");
    cli.executeCommand("cc /sys$topicmanager/topics");
    cli.executeCommand("new " + topicName);
  }

  public void deleteQueue(String queueName) throws Exception
  {
    if (cli == null)
      createCLI();
    cli.waitForRouter("router");
    cli.executeCommand("sr router");
    cli.executeCommand("cc /sys$queuemanager/queues");
    cli.executeCommand("delete " + queueName);
  }

  public void deleteTopic(String topicName) throws Exception
  {
    if (cli == null)
      createCLI();
    cli.waitForRouter("router");
    cli.executeCommand("sr router");
    cli.executeCommand("cc /sys$topicmanager/topics");
    cli.executeCommand("delete " + topicName);
  }

  public void haltRouter(String routerName) throws Exception
  {
    if (cli == null)
      createCLI();
    cli.waitForRouter(routerName);
    cli.executeCommand("sr " + routerName);
    cli.executeCommand("halt");
  }

  public void rebootRouter(String routerName) throws Exception
  {
    if (cli == null)
      createCLI();
    cli.waitForRouter(routerName);
    cli.executeCommand("sr " + routerName);
    cli.executeCommand("reboot");
  }

  public void saveRouter(String routerName) throws Exception
  {
    if (cli == null)
      createCLI();
    cli.waitForRouter(routerName);
    cli.executeCommand("sr " + routerName);
    cli.executeCommand("save");
  }

  public static void failFast(String msg){
    fail(msg);
    System.exit(-1);
  }

  protected void tearDown() throws Exception
  {
    if (cli != null)
    {
      cli.close();
      cliQC.close();
      cli = null;
      cliQC = null;
    }
    super.tearDown();
  }

  protected static synchronized int nextId()
  {
    return id++;
  }
}

