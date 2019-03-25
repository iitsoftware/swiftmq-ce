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

package com.swiftmq.filetransfer.protocol.v940;

import com.swiftmq.filetransfer.protocol.MessageBasedReply;
import com.swiftmq.filetransfer.protocol.MessageBasedRequest;
import com.swiftmq.filetransfer.protocol.MessageBasedRequestVisitor;
import com.swiftmq.jms.MessageImpl;

import javax.jms.JMSException;
import javax.jms.Message;

public class FilePublishRequest extends MessageBasedRequest {
    public static final String REPLYQUEUE_PROP = "JMS_SWIFTMQ_FT_REPLYQUEUE";
    public static final String FILENAME_PROP = "JMS_SWIFTMQ_FT_FILENAME";
    public static final String SIZE_PROP = "JMS_SWIFTMQ_FT_SIZE";
    public static final String EXPIRATION_PROP = "JMS_SWIFTMQ_FT_EXPIRATION";
    public static final String DELAFTERDL_PROP = "JMS_SWIFTMQ_FT_DELAFTDL";
    public static final String DIGESTTYPE_PROP = "JMS_SWIFTMQ_FT_DIGESTTYPE";
    public static final String PWDHEXDIGEST_PROP = "JMS_SWIFTMQ_FT_PWDHEXDIGEST";
    public static final String FILEISPRIVATE_PROP = "JMS_SWIFTMQ_FT_FILEISPRIVATE";
    public static final String USERNAME_PROP = "JMS_SWIFTMQ_FT_USERNAME";

    String replyQueue = null;
    String filename = null;
    String username = null;
    long size = 0;
    long expiration = 0;
    int deleteAfterNumberDownloads = 0;
    String digestType = null;
    String passwordHexDigest = null;
    boolean fileIsPrivate = false;

    public FilePublishRequest(Message message) throws JMSException {
        super(message);
        replyQueue = message.getStringProperty(REPLYQUEUE_PROP);
        filename = message.getStringProperty(FILENAME_PROP);
        username = message.getStringProperty(USERNAME_PROP);
        size = message.getLongProperty(SIZE_PROP);
        expiration = message.getLongProperty(EXPIRATION_PROP);
        deleteAfterNumberDownloads = message.getIntProperty(DELAFTERDL_PROP);
        digestType = message.getStringProperty(DIGESTTYPE_PROP);
        if (message.propertyExists(PWDHEXDIGEST_PROP))
            passwordHexDigest = message.getStringProperty(PWDHEXDIGEST_PROP);
        fileIsPrivate = message.getBooleanProperty(FILEISPRIVATE_PROP);
    }

    public FilePublishRequest(String replyQueue, String filename, String username, long size, long expiration, int deleteAfterNumberDownloads, String digestType, String passwordHexDigest, boolean fileIsPrivate) {
        setReplyRequired(true);
        this.replyQueue = replyQueue;
        this.filename = filename;
        this.username = username;
        this.size = size;
        this.expiration = expiration;
        this.deleteAfterNumberDownloads = deleteAfterNumberDownloads;
        this.digestType = digestType;
        this.passwordHexDigest = passwordHexDigest;
        this.fileIsPrivate = fileIsPrivate;
    }

    public String getReplyQueue() {
        return replyQueue;
    }

    public String getFilename() {
        return filename;
    }

    public String getUsername() {
        return username;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    public int getDeleteAfterNumberDownloads() {
        return deleteAfterNumberDownloads;
    }

    public void setDeleteAfterNumberDownloads(int deleteAfterNumberDownloads) {
        this.deleteAfterNumberDownloads = deleteAfterNumberDownloads;
    }

    public String getDigestType() {
        return digestType;
    }

    public void setDigestType(String digestType) {
        this.digestType = digestType;
    }

    public String getPasswordHexDigest() {
        return passwordHexDigest;
    }

    public void setPasswordHexDigest(String passwordHexDigest) {
        this.passwordHexDigest = passwordHexDigest;
    }

    public boolean isFileIsPrivate() {
        return fileIsPrivate;
    }

    public void setFileIsPrivate(boolean fileIsPrivate) {
        this.fileIsPrivate = fileIsPrivate;
    }

    public MessageBasedReply createReplyInstance() {
        return new FilePublishReply();
    }

    public void accept(MessageBasedRequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public Message toMessage() throws JMSException {
        Message message = new MessageImpl();
        fillMessage(message);
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILEPUBLISH_REQ);
        message.setStringProperty(REPLYQUEUE_PROP, replyQueue);
        message.setStringProperty(FILENAME_PROP, filename);
        if (username != null)
            message.setStringProperty(USERNAME_PROP, username);
        message.setLongProperty(SIZE_PROP, size);
        message.setLongProperty(EXPIRATION_PROP, expiration);
        message.setStringProperty(DIGESTTYPE_PROP, digestType);
        if (passwordHexDigest != null)
            message.setStringProperty(PWDHEXDIGEST_PROP, passwordHexDigest);
        message.setIntProperty(DELAFTERDL_PROP, deleteAfterNumberDownloads);
        message.setBooleanProperty(FILEISPRIVATE_PROP, fileIsPrivate);
        return message;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FilePublishRequest");
        sb.append(", replyQueue='").append(replyQueue).append('\'');
        sb.append(", filename='").append(filename).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", size=").append(size);
        sb.append(", expiration=").append(expiration);
        sb.append(", deleteAfterNumberDownloads=").append(deleteAfterNumberDownloads);
        sb.append(", digestType=").append(digestType);
        sb.append(", passwordHexDigest=").append(passwordHexDigest);
        sb.append(", fileIsPrivate=").append(fileIsPrivate);
        sb.append(']');
        return sb.toString();
    }
}
