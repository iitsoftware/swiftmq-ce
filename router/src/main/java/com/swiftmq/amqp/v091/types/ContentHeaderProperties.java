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

package com.swiftmq.amqp.v091.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class ContentHeaderProperties {
    Integer classId = null;
    Integer weight = null;
    Long bodySize = null;
    String contentType = null;
    String contentEncoding = null;
    Map<String, Object> headers = null;
    Integer deliveryMode = null;
    Integer priority = null;
    String correlationId = null;
    String replyTo = null;
    String expiration = null;
    String messageId = null;
    Long timestamp = null;
    String type = null;
    String userId = null;
    String appId = null;
    String clusterId = null;
    int flags = 0;
    int cnt = 0;

    public Integer getClassId() {
        return classId;
    }

    public void setClassId(Integer classId) {
        this.classId = classId;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    public Long getBodySize() {
        return bodySize;
    }

    public void setBodySize(Long bodySize) {
        this.bodySize = bodySize;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public void setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public Integer getDeliveryMode() {
        return deliveryMode;
    }

    public void setDeliveryMode(Integer deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    public String getExpiration() {
        return expiration;
    }

    public void setExpiration(String expiration) {
        this.expiration = expiration;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    private void setFlag(boolean set) {
        if (set)
            flags = flags | (1 << (15 - cnt));
        cnt++;
    }

    public void writeContent(DataOutput out) throws IOException {
        Coder.writeShort(classId, out);
        Coder.writeShort(weight, out);
        Coder.writeLong(bodySize, out);
        flags = 0;
        setFlag(contentType != null);
        setFlag(contentEncoding != null);
        setFlag(headers != null && headers.size() > 0);
        setFlag(deliveryMode != null);
        setFlag(priority != null);
        setFlag(correlationId != null);
        setFlag(replyTo != null);
        setFlag(expiration != null);
        setFlag(messageId != null);
        setFlag(timestamp != null);
        setFlag(type != null);
        setFlag(userId != null);
        setFlag(appId != null);
        setFlag(clusterId != null);
        Coder.writeShort(flags, out);

        if (contentType != null) Coder.writeShortString(contentType, out);
        if (contentEncoding != null) Coder.writeShortString(contentEncoding, out);
        if (headers != null && headers.size() > 0) Coder.writeTable(headers, out);
        if (deliveryMode != null) Coder.writeByte(deliveryMode, out);
        if (priority != null) Coder.writeByte(priority, out);
        if (correlationId != null) Coder.writeShortString(correlationId, out);
        if (replyTo != null) Coder.writeShortString(replyTo, out);
        if (expiration != null) Coder.writeShortString(expiration, out);
        if (messageId != null) Coder.writeShortString(messageId, out);
        if (timestamp != null) Coder.writeLong(timestamp, out);
        if (type != null) Coder.writeShortString(type, out);
        if (userId != null) Coder.writeShortString(userId, out);
        if (appId != null) Coder.writeShortString(appId, out);
        if (clusterId != null) Coder.writeShortString(clusterId, out);
    }

    private boolean isFlagSet(int bit) {
        bit = 15 - bit;
        return (flags & (1 << bit)) != 0;
    }

    public void readContent(DataInput in) throws IOException {
        classId = new Integer(Coder.readShort(in));
        weight = new Integer(Coder.readShort(in));
        bodySize = new Long(Coder.readLong(in));
        flags = Coder.readShort(in);
        if (isFlagSet(0)) contentType = Coder.readShortString(in);
        if (isFlagSet(1)) contentEncoding = Coder.readShortString(in);
        if (isFlagSet(2)) headers = Coder.readTable(in);
        if (isFlagSet(3)) deliveryMode = new Integer(Coder.readByte(in));
        if (isFlagSet(4)) priority = new Integer(Coder.readByte(in));
        if (isFlagSet(5)) correlationId = Coder.readShortString(in);
        if (isFlagSet(6)) replyTo = Coder.readShortString(in);
        if (isFlagSet(7)) expiration = Coder.readShortString(in);
        if (isFlagSet(8)) messageId = Coder.readShortString(in);
        if (isFlagSet(9)) timestamp = Coder.readLong(in);
        if (isFlagSet(10)) type = Coder.readShortString(in);
        if (isFlagSet(11)) userId = Coder.readShortString(in);
        if (isFlagSet(12)) appId = Coder.readShortString(in);
        if (isFlagSet(13)) clusterId = Coder.readShortString(in);
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("[ContentHeaderProperties");
        sb.append(" classId=").append(classId);
        sb.append(", weight=").append(weight);
        sb.append(", bodySize=").append(bodySize);
        sb.append(", contentType='").append(contentType).append('\'');
        sb.append(", contentEncoding='").append(contentEncoding).append('\'');
        sb.append(", headers=").append(headers);
        sb.append(", deliveryMode=").append(deliveryMode);
        sb.append(", priority=").append(priority);
        sb.append(", correlationId='").append(correlationId).append('\'');
        sb.append(", replyTo='").append(replyTo).append('\'');
        sb.append(", expiration='").append(expiration).append('\'');
        sb.append(", messageId='").append(messageId).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", type='").append(type).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", appId='").append(appId).append('\'');
        sb.append(", clusterId='").append(clusterId).append('\'');
        sb.append(", flags=").append(flags);
        sb.append(", cnt=").append(cnt);
        sb.append(']');
        return sb.toString();
    }
}
