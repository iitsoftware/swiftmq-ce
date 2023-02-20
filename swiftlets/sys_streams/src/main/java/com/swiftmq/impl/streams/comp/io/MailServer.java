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

package com.swiftmq.impl.streams.comp.io;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;

import javax.mail.*;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Represents a Connection to a Mail Server.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class MailServer {
    StreamContext ctx;
    String host;
    int port = 25;
    String username;
    String password;
    Session session;
    Transport transport;
    Entity usage;
    int nSent = 0;

    /**
     * Internal use.
     */
    public MailServer(StreamContext ctx, String host) {
        this.ctx = ctx;
        this.host = host;
    }

    /**
     * Sets the port (default 25 = SMTP)
     *
     * @param port Port
     * @return MailServer
     */
    public MailServer port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the user name.
     *
     * @param username User Name
     * @return MailServer
     */
    public MailServer username(String username) {
        this.username = username;
        return this;
    }

    /**
     * Sets the password.
     *
     * @param password Password
     * @return MailServer
     */
    public MailServer password(String password) {
        this.password = password;
        return this;
    }

    private void _connect(boolean ssl) throws Exception {
        boolean smtpAuthEnabled = username != null;
        Properties props = new Properties();
        props.setProperty("mail.smtp.host", host);
        props.setProperty("mail.smtp.auth", String.valueOf(smtpAuthEnabled));
        if (ssl)
            props.setProperty("mail.smtp.ssl.enable", "true");
        try {
            session = Session.getInstance(props, smtpAuthEnabled ? new SMTPPasswordAuthenticator(username, password) : null);
            session.setDebug(ctx.ctx.traceSpace.enabled);
            transport = session.getTransport(ssl ? "smtps" : "smtp");
            transport.connect();
            EntityList mailList = (EntityList) ctx.usage.getEntity("mailservers");
            usage = mailList.createEntity();
            usage.setName(host);
            usage.createCommands();
            mailList.addEntity(usage);
        } catch (Exception e) {
            disconnect();
            throw e;
        }
    }

    /**
     * Does the actual connect to the mail server.
     *
     * @return MailServer
     * @throws Exception
     */
    public MailServer connect() throws Exception {
        try {
            _connect(false);
        } catch (Exception e) {
            ctx.stream.log().warning("Connect non-ssl failed(" + e.getMessage() + "), trying ssl ...");
            _connect(true);
        }
        return this;
    }

    /**
     * Disconnects from the mail server.
     *
     * @return MailServer
     */
    public MailServer disconnect() {
        try {
            ctx.usage.getEntity("mailservers").removeEntity(usage);
            if (transport != null)
                transport.close();
        } catch (Exception e) {

        }
        return this;
    }

    MimeMessage createMessage() throws Exception {
        if (transport == null)
            connect();
        return new MimeMessage(session);
    }

    MailServer send(MimeMessage mimeMessage) throws Exception {
        int cnt = 2;
        while (cnt > 0) {
            if (transport == null)
                connect();
            try {
                transport.sendMessage(mimeMessage, mimeMessage.getAllRecipients());
                cnt = 0;
            } catch (MessagingException e) {
                try {
                    transport.close();
                } catch (MessagingException e1) {
                }
                transport = null;
                cnt--;
                if (cnt == 0)
                    throw e;
            }
        }
        nSent++;
        if (nSent == Integer.MAX_VALUE)
            nSent = 0;
        return this;
    }

    /**
     * Returns a new Email facade to send a mail.
     *
     * @return Email
     * @throws Exception
     */
    public Email email() throws Exception {
        return new Email(ctx, this);
    }

    /**
     * Internal use.
     */
    public void collect(long interval) {
        try {
            if (usage == null)
                return;
            if (nSent > 0)
                usage.getProperty("emails-sent").setValue(nSent);
            else
                usage.getProperty("emails-sent").setValue(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Closes this MailServer.
     */
    public void close() {
        try {
            disconnect();
            if (usage != null)
                ctx.usage.getEntity("mailservers").removeEntity(usage);
        } catch (Exception e) {
        }
        ctx.stream.removeMailServer(host);
    }

    private class SMTPPasswordAuthenticator extends Authenticator {
        String username = null;
        String password = null;

        public SMTPPasswordAuthenticator(String username, String password) {
            this.username = username;
            this.password = password;
        }

        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(username, password);
        }
    }
}
