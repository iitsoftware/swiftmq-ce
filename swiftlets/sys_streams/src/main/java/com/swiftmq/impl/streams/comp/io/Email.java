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

import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Facade to send an eMail.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class Email {
    StreamContext ctx;
    MailServer mailServer;
    String from;
    String to;
    String cc;
    String bcc;
    String subject;
    String body;

    Email(StreamContext ctx, MailServer mailServer) {
        this.ctx = ctx;
        this.mailServer = mailServer;
    }

    /**
     * Sets the from address
     *
     * @param from address
     * @return Email
     */
    public Email from(String from) {
        this.from = from;
        return this;
    }

    /**
     * Sets the to address
     *
     * @param to address
     * @return Email
     */
    public Email to(String to) {
        this.to = to;
        return this;
    }

    /**
     * Sets the jms.cc address
     *
     * @param cc address
     * @return Email
     */
    public Email cc(String cc) {
        this.cc = cc;
        return this;
    }

    /**
     * Sets the bcc address
     *
     * @param bcc address
     * @return Email
     */
    public Email bcc(String bcc) {
        this.bcc = bcc;
        return this;
    }

    /**
     * Sets the subject
     *
     * @param subject subject
     * @return Email
     */
    public Email subject(String subject) {
        this.subject = subject;
        return this;
    }

    /**
     * Sets the mail body.
     *
     * @param body mail body
     * @return Email
     */
    public Email body(String body) {
        this.body = body;
        return this;
    }

    /**
     * Sets fields according to its name: from, to, jms.cc, bcc, subject, body
     *
     * @param field field name
     * @param value field value
     * @return this
     */
    public Email set(String field, String value) {
        if (field.equals("from"))
            from(value);
        else if (field.equals("to"))
            to(value);
        else if (field.equals("cc"))
            cc(value);
        else if (field.equals("bcc"))
            bcc(value);
        else if (field.equals("subject"))
            subject(value);
        else if (field.equals("body"))
            body(value);
        return this;
    }

    /**
     * Sends the eMail message.
     *
     * @throws Exception
     */
    public void send() throws Exception {
        MimeMessage message = mailServer.createMessage();
        message.setFrom(new InternetAddress(from));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
        if (cc != null && cc.trim().length() > 0)
            message.addRecipient(Message.RecipientType.CC, new InternetAddress(cc));
        if (bcc != null && bcc.trim().length() > 0)
            message.addRecipient(Message.RecipientType.BCC, new InternetAddress(bcc));
        message.setSubject(subject);
        message.setText(body);
        mailServer.send(message);

    }
}
