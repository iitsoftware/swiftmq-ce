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

package com.swiftmq.swiftlet.monitor;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.QueueManager;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.text.SimpleDateFormat;
import java.util.*;

public class MailGenerator
{
  static final String CRLF = "\r\n";
  static SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
  SwiftletContext ctx = null;
  QueueManager queueManager = null;
  List collectors = new ArrayList();
  Property mailHost = null;
  Property mailPort = null;
  Property smtpAuth = null;
  Property username = null;
  Property password = null;
  Property from = null;
  Property to = null;
  Property cc = null;
  Exception lastException = null;

  public MailGenerator(SwiftletContext ctx)
  {
    this.ctx = ctx;
    queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    collectors.add(new QueueCollector(ctx));
    collectors.add(new ConnectionCollector(ctx));
    Entity entity = ctx.root.getEntity("settingsmail");
    mailHost = entity.getProperty("mailserver-host");
    mailPort = entity.getProperty("mailserver-port");
    smtpAuth = entity.getProperty("mailserver-authentication-enabled");
    username = entity.getProperty("mailserver-username");
    password = entity.getProperty("mailserver-password");
    from = entity.getProperty("from");
    to = entity.getProperty("to");
    cc = entity.getProperty("cc");
    CommandRegistry commandRegistry = entity.getCommandRegistry();
    CommandExecutor commitExecutor = new CommandExecutor()
    {
      public String[] execute(String[] context, Entity entity, String[] cmd)
      {
        if (cmd.length != 1)
          return new String[]{TreeCommands.ERROR, "Invalid command, please try 'testmail'"};
        generateMail("[Testmail] Memory Monitor Swiftlet on " + SwiftletManager.getInstance().getRouterName());
        String[] result = null;
        if (lastException != null)
          result = new String[]{TreeCommands.ERROR, "Exception sending Testmail: " + lastException};
        else
          result = new String[]{TreeCommands.INFO, "Testmail sent!"};
        return result;
      }
    };
    Command command = new Command("testmail", "testmail", "Send a Testmail", true, commitExecutor, true, false);
    commandRegistry.addCommand(command);

    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/created");
  }

  public void generateMail(String subject)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/generateMail, subject=" + subject);
    try
    {
      lastException = null;
      StringBuffer b = new StringBuffer("ROUTER MEMORY STATUS:\r\n\r\n");
      long total = Runtime.getRuntime().totalMemory() / (1024 * 1024);
      long free = Runtime.getRuntime().freeMemory() / (1024 * 1024);
      long max = Runtime.getRuntime().maxMemory() / (1024 * 1024);
      b.append("Max Memory: ");
      b.append(max);
      b.append(" MB\r\n");
      b.append("Total Memory: ");
      b.append(total);
      b.append(" MB\r\n");
      b.append("Free Memory: ");
      b.append(free);
      b.append(" MB\r\n\r\n\r\n");
      for (int i = 0; i < collectors.size(); i++)
      {
        Collector c = (Collector) collectors.get(i);
        Map map = c.collect();
        b.append(c.getDescription());
        b.append("\r\n\r\n");
        String[] columns = c.getColumnNames();
        b.append(columns[0]);
        b.append(" / ");
        b.append(columns[1]);
        b.append("\r\n\r\n");
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); )
        {
          Map.Entry entry = (Map.Entry) iter.next();
          b.append(entry.getKey());
          b.append(" / ");
          b.append(entry.getValue().toString());
          b.append("\r\n");
        }
        b.append("\r\n\r\n");
      }
      sendMail((String) from.getValue(), (String) to.getValue(), (String) cc.getValue(), subject, b.toString());
    } catch (Exception e)
    {
      lastException = e;
      ctx.logSwiftlet.logError(ctx.swiftlet.getName(), toString() + "/generateMail, exception=" + e);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/generateMail, exception=" + e);
    }
  }

  public void generateMail(String subject, String body)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/generateMail, subject=" + subject + ", body=" + body);
    try
    {
      sendMail((String) from.getValue(), (String) to.getValue(), (String) cc.getValue(), subject, body);
    } catch (Exception e)
    {
      ctx.logSwiftlet.logError(ctx.swiftlet.getName(), toString() + "/generateMail, exception=" + e);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/generateMail, exception=" + e);
    }
  }

  private void sendMail(String from, String to, String cc, String subject, String body) throws Exception
  {
    String smtpHost = (String) mailHost.getValue();
    int smtpPort = ((Integer) mailPort.getValue()).intValue();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "try to connect to " + smtpHost + ':' + smtpPort + "...");
    boolean smtpAuthEnabled = ((Boolean) smtpAuth.getValue()).booleanValue();
    Properties props = new Properties();
    props.setProperty("mail.smtp.host", smtpHost);
    props.setProperty("mail.smtp.auth", String.valueOf(smtpAuthEnabled));
    Session session = Session.getInstance(props, smtpAuthEnabled ? new SMTPPasswordAuthenticator(username, password) : null);
    session.setDebug(ctx.traceSpace.enabled);
    MimeMessage message = new MimeMessage(session);
    message.setFrom(new InternetAddress(from));
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
    if (cc != null && cc.trim().length() > 0)
      message.addRecipient(Message.RecipientType.CC, new InternetAddress(cc));
    message.setSubject(subject);
    message.setText(body);
    Transport.send(message);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "mail successfully sent.");
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/close");
    collectors.clear();
  }

  public String toString()
  {
    return "MailGenerator";
  }

  public class SMTPPasswordAuthenticator extends javax.mail.Authenticator
  {
    Property usernameProp = null;
    Property passwordProp = null;

    public SMTPPasswordAuthenticator(Property usernameProp, Property passwordProp)
    {
      this.usernameProp = usernameProp;
      this.passwordProp = passwordProp;
    }

    protected PasswordAuthentication getPasswordAuthentication()
    {
      return new PasswordAuthentication((String) usernameProp.getValue(), (String) passwordProp.getValue());
    }
  }
}
