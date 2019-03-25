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

package com.swiftmq.jndi.v400;

import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TopicImpl;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

public class SwiftMQObjectFactory implements ObjectFactory {
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable environment) throws Exception {
        if (obj instanceof Reference) {
            Reference ref = (Reference) obj;
            if (ref.getClassName().equals(TopicImpl.class.getName())) {
                RefAddr addr = ref.get("topicName");
                if (addr != null) {
                    return new TopicImpl((String) addr.getContent());
                }
            } else if (ref.getClassName().equals(QueueImpl.class.getName())) {
                RefAddr addr = ref.get("queueName");
                if (addr != null) {
                    return new QueueImpl((String) addr.getContent());
                }
            } else if (ref.getClassName().equals(com.swiftmq.jms.v400.ConnectionFactoryImpl.class.getName())) {
                RefAddr listenerName = ref.get("listenerName");
                RefAddr socketFactoryClass = ref.get("socketFactoryClass");
                RefAddr hostname = ref.get("hostname");
                RefAddr port = ref.get("port");
                RefAddr keepaliveInterval = ref.get("keepaliveInterval");
                RefAddr clientId = ref.get("clientId");
                RefAddr smqpProducerReplyInterval = ref.get("smqpProducerReplyInterval");
                RefAddr smqpConsumerCacheSize = ref.get("smqpConsumerCacheSize");
                RefAddr jmsDeliveryMode = ref.get("jmsDeliveryMode");
                RefAddr jmsPriority = ref.get("jmsPriority");
                RefAddr jmsTTL = ref.get("jmsTTL");
                RefAddr jmsMessageIdEnabled = ref.get("jmsMessageIdEnabled");
                RefAddr jmsMessageTimestampEnabled = ref.get("jmsMessageTimestampEnabled");
                RefAddr useThreadContextCL = ref.get("useThreadContextCL");
                RefAddr inputBufferSize = ref.get("inputBufferSize");
                RefAddr inputExtendSize = ref.get("inputExtendSize");
                RefAddr outputBufferSize = ref.get("outputBufferSize");
                RefAddr outputExtendSize = ref.get("outputExtendSize");
                RefAddr intraVM = ref.get("intraVM");
                if (listenerName != null &&
                        socketFactoryClass != null &&
                        hostname != null &&
                        port != null &&
                        keepaliveInterval != null &&
                        smqpProducerReplyInterval != null &&
                        smqpConsumerCacheSize != null &&
                        jmsDeliveryMode != null &&
                        jmsPriority != null &&
                        jmsTTL != null &&
                        jmsMessageIdEnabled != null &&
                        jmsMessageTimestampEnabled != null &&
                        useThreadContextCL != null &&
                        inputBufferSize != null &&
                        inputExtendSize != null &&
                        outputBufferSize != null &&
                        outputExtendSize != null &&
                        intraVM != null) {
                    com.swiftmq.jms.v400.ConnectionFactoryImpl cf = new com.swiftmq.jms.v400.ConnectionFactoryImpl((String) listenerName.getContent(),
                            (String) socketFactoryClass.getContent(),
                            (String) hostname.getContent(),
                            Integer.parseInt((String) port.getContent()),
                            Long.parseLong((String) keepaliveInterval.getContent()),
                            clientId == null ? null : (String) clientId.getContent(),
                            Integer.parseInt((String) smqpProducerReplyInterval.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSize.getContent()),
                            Integer.parseInt((String) jmsDeliveryMode.getContent()),
                            Integer.parseInt((String) jmsPriority.getContent()),
                            Long.parseLong((String) jmsTTL.getContent()),
                            Boolean.valueOf((String) jmsMessageIdEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) jmsMessageTimestampEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) useThreadContextCL.getContent()).booleanValue(),
                            Integer.parseInt((String) inputBufferSize.getContent()),
                            Integer.parseInt((String) inputExtendSize.getContent()),
                            Integer.parseInt((String) outputBufferSize.getContent()),
                            Integer.parseInt((String) outputExtendSize.getContent()),
                            Boolean.valueOf((String) intraVM.getContent()).booleanValue());
                    return cf;
                }
            } else if (ref.getClassName().equals(com.swiftmq.jms.v500.ConnectionFactoryImpl.class.getName())) {
                RefAddr listenerName = ref.get("listenerName");
                RefAddr socketFactoryClass = ref.get("socketFactoryClass");
                RefAddr hostname = ref.get("hostname");
                RefAddr port = ref.get("port");
                RefAddr keepaliveInterval = ref.get("keepaliveInterval");
                RefAddr clientId = ref.get("clientId");
                RefAddr smqpProducerReplyInterval = ref.get("smqpProducerReplyInterval");
                RefAddr smqpConsumerCacheSize = ref.get("smqpConsumerCacheSize");
                RefAddr jmsDeliveryMode = ref.get("jmsDeliveryMode");
                RefAddr jmsPriority = ref.get("jmsPriority");
                RefAddr jmsTTL = ref.get("jmsTTL");
                RefAddr jmsMessageIdEnabled = ref.get("jmsMessageIdEnabled");
                RefAddr jmsMessageTimestampEnabled = ref.get("jmsMessageTimestampEnabled");
                RefAddr useThreadContextCL = ref.get("useThreadContextCL");
                RefAddr inputBufferSize = ref.get("inputBufferSize");
                RefAddr inputExtendSize = ref.get("inputExtendSize");
                RefAddr outputBufferSize = ref.get("outputBufferSize");
                RefAddr outputExtendSize = ref.get("outputExtendSize");
                RefAddr intraVM = ref.get("intraVM");
                if (listenerName != null &&
                        socketFactoryClass != null &&
                        hostname != null &&
                        port != null &&
                        keepaliveInterval != null &&
                        smqpProducerReplyInterval != null &&
                        smqpConsumerCacheSize != null &&
                        jmsDeliveryMode != null &&
                        jmsPriority != null &&
                        jmsTTL != null &&
                        jmsMessageIdEnabled != null &&
                        jmsMessageTimestampEnabled != null &&
                        useThreadContextCL != null &&
                        inputBufferSize != null &&
                        inputExtendSize != null &&
                        outputBufferSize != null &&
                        outputExtendSize != null &&
                        intraVM != null) {
                    com.swiftmq.jms.v500.ConnectionFactoryImpl cf = new com.swiftmq.jms.v500.ConnectionFactoryImpl((String) listenerName.getContent(),
                            (String) socketFactoryClass.getContent(),
                            (String) hostname.getContent(),
                            Integer.parseInt((String) port.getContent()),
                            Long.parseLong((String) keepaliveInterval.getContent()),
                            clientId == null ? null : (String) clientId.getContent(),
                            Integer.parseInt((String) smqpProducerReplyInterval.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSize.getContent()),
                            Integer.parseInt((String) jmsDeliveryMode.getContent()),
                            Integer.parseInt((String) jmsPriority.getContent()),
                            Long.parseLong((String) jmsTTL.getContent()),
                            Boolean.valueOf((String) jmsMessageIdEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) jmsMessageTimestampEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) useThreadContextCL.getContent()).booleanValue(),
                            Integer.parseInt((String) inputBufferSize.getContent()),
                            Integer.parseInt((String) inputExtendSize.getContent()),
                            Integer.parseInt((String) outputBufferSize.getContent()),
                            Integer.parseInt((String) outputExtendSize.getContent()),
                            Boolean.valueOf((String) intraVM.getContent()).booleanValue());
                    return cf;
                }
            } else if (ref.getClassName().equals(com.swiftmq.jms.v510.ConnectionFactoryImpl.class.getName())) {
                RefAddr listenerName = ref.get("listenerName");
                RefAddr socketFactoryClass = ref.get("socketFactoryClass");
                RefAddr hostname = ref.get("hostname");
                RefAddr port = ref.get("port");
                RefAddr keepaliveInterval = ref.get("keepaliveInterval");
                RefAddr clientId = ref.get("clientId");
                RefAddr smqpProducerReplyInterval = ref.get("smqpProducerReplyInterval");
                RefAddr smqpConsumerCacheSize = ref.get("smqpConsumerCacheSize");
                RefAddr jmsDeliveryMode = ref.get("jmsDeliveryMode");
                RefAddr jmsPriority = ref.get("jmsPriority");
                RefAddr jmsTTL = ref.get("jmsTTL");
                RefAddr jmsMessageIdEnabled = ref.get("jmsMessageIdEnabled");
                RefAddr jmsMessageTimestampEnabled = ref.get("jmsMessageTimestampEnabled");
                RefAddr useThreadContextCL = ref.get("useThreadContextCL");
                RefAddr inputBufferSize = ref.get("inputBufferSize");
                RefAddr inputExtendSize = ref.get("inputExtendSize");
                RefAddr outputBufferSize = ref.get("outputBufferSize");
                RefAddr outputExtendSize = ref.get("outputExtendSize");
                RefAddr intraVM = ref.get("intraVM");
                if (listenerName != null &&
                        socketFactoryClass != null &&
                        hostname != null &&
                        port != null &&
                        keepaliveInterval != null &&
                        smqpProducerReplyInterval != null &&
                        smqpConsumerCacheSize != null &&
                        jmsDeliveryMode != null &&
                        jmsPriority != null &&
                        jmsTTL != null &&
                        jmsMessageIdEnabled != null &&
                        jmsMessageTimestampEnabled != null &&
                        useThreadContextCL != null &&
                        inputBufferSize != null &&
                        inputExtendSize != null &&
                        outputBufferSize != null &&
                        outputExtendSize != null &&
                        intraVM != null) {
                    com.swiftmq.jms.v510.ConnectionFactoryImpl cf = new com.swiftmq.jms.v510.ConnectionFactoryImpl((String) listenerName.getContent(),
                            (String) socketFactoryClass.getContent(),
                            (String) hostname.getContent(),
                            Integer.parseInt((String) port.getContent()),
                            Long.parseLong((String) keepaliveInterval.getContent()),
                            clientId == null ? null : (String) clientId.getContent(),
                            Integer.parseInt((String) smqpProducerReplyInterval.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSize.getContent()),
                            Integer.parseInt((String) jmsDeliveryMode.getContent()),
                            Integer.parseInt((String) jmsPriority.getContent()),
                            Long.parseLong((String) jmsTTL.getContent()),
                            Boolean.valueOf((String) jmsMessageIdEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) jmsMessageTimestampEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) useThreadContextCL.getContent()).booleanValue(),
                            Integer.parseInt((String) inputBufferSize.getContent()),
                            Integer.parseInt((String) inputExtendSize.getContent()),
                            Integer.parseInt((String) outputBufferSize.getContent()),
                            Integer.parseInt((String) outputExtendSize.getContent()),
                            Boolean.valueOf((String) intraVM.getContent()).booleanValue());
                    return cf;
                }
            } else if (ref.getClassName().equals(com.swiftmq.jms.v600.ConnectionFactoryImpl.class.getName())) {
                RefAddr listenerName = ref.get("listenerName");
                RefAddr socketFactoryClass = ref.get("socketFactoryClass");
                RefAddr hostname = ref.get("hostname");
                RefAddr port = ref.get("port");
                RefAddr keepaliveInterval = ref.get("keepaliveInterval");
                RefAddr clientId = ref.get("clientId");
                RefAddr smqpProducerReplyInterval = ref.get("smqpProducerReplyInterval");
                RefAddr smqpConsumerCacheSize = ref.get("smqpConsumerCacheSize");
                RefAddr jmsDeliveryMode = ref.get("jmsDeliveryMode");
                RefAddr jmsPriority = ref.get("jmsPriority");
                RefAddr jmsTTL = ref.get("jmsTTL");
                RefAddr jmsMessageIdEnabled = ref.get("jmsMessageIdEnabled");
                RefAddr jmsMessageTimestampEnabled = ref.get("jmsMessageTimestampEnabled");
                RefAddr useThreadContextCL = ref.get("useThreadContextCL");
                RefAddr inputBufferSize = ref.get("inputBufferSize");
                RefAddr inputExtendSize = ref.get("inputExtendSize");
                RefAddr outputBufferSize = ref.get("outputBufferSize");
                RefAddr outputExtendSize = ref.get("outputExtendSize");
                RefAddr intraVM = ref.get("intraVM");
                RefAddr hostname2 = ref.get("hostname2");
                RefAddr port2 = ref.get("port2");
                RefAddr reconnectEnabled = ref.get("reconnectEnabled");
                RefAddr maxRetries = ref.get("maxRetries");
                RefAddr retryDelay = ref.get("retryDelay");
                RefAddr duplicateMessageDetection = ref.get("duplicateMessageDetection");
                RefAddr duplicateBacklogSize = ref.get("duplicateBacklogSize");
                if (listenerName != null &&
                        socketFactoryClass != null &&
                        hostname != null &&
                        port != null &&
                        keepaliveInterval != null &&
                        smqpProducerReplyInterval != null &&
                        smqpConsumerCacheSize != null &&
                        jmsDeliveryMode != null &&
                        jmsPriority != null &&
                        jmsTTL != null &&
                        jmsMessageIdEnabled != null &&
                        jmsMessageTimestampEnabled != null &&
                        useThreadContextCL != null &&
                        inputBufferSize != null &&
                        inputExtendSize != null &&
                        outputBufferSize != null &&
                        outputExtendSize != null &&
                        intraVM != null) {
                    com.swiftmq.jms.v600.ConnectionFactoryImpl cf = new com.swiftmq.jms.v600.ConnectionFactoryImpl((String) listenerName.getContent(),
                            (String) socketFactoryClass.getContent(),
                            (String) hostname.getContent(),
                            Integer.parseInt((String) port.getContent()),
                            Long.parseLong((String) keepaliveInterval.getContent()),
                            clientId == null ? null : (String) clientId.getContent(),
                            Integer.parseInt((String) smqpProducerReplyInterval.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSize.getContent()),
                            Integer.parseInt((String) jmsDeliveryMode.getContent()),
                            Integer.parseInt((String) jmsPriority.getContent()),
                            Long.parseLong((String) jmsTTL.getContent()),
                            Boolean.valueOf((String) jmsMessageIdEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) jmsMessageTimestampEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) useThreadContextCL.getContent()).booleanValue(),
                            Integer.parseInt((String) inputBufferSize.getContent()),
                            Integer.parseInt((String) inputExtendSize.getContent()),
                            Integer.parseInt((String) outputBufferSize.getContent()),
                            Integer.parseInt((String) outputExtendSize.getContent()),
                            Boolean.valueOf((String) intraVM.getContent()).booleanValue());
                    cf.setReconnectEnabled(Boolean.valueOf((String) reconnectEnabled.getContent()).booleanValue());
                    cf.setMaxRetries(Integer.valueOf((String) maxRetries.getContent()).intValue());
                    cf.setRetryDelay(Long.valueOf((String) retryDelay.getContent()).longValue());
                    cf.setDuplicateMessageDetection(Boolean.valueOf((String) duplicateMessageDetection.getContent()).booleanValue());
                    cf.setDuplicateBacklogSize(Integer.valueOf((String) duplicateBacklogSize.getContent()).intValue());

                    if (hostname2 != null) {
                        cf.setHostname2((String) hostname2.getContent());
                        cf.setPort2(Integer.parseInt((String) port2.getContent()));
                    }
                    return cf;
                }
            } else if (ref.getClassName().equals(com.swiftmq.jms.v610.ConnectionFactoryImpl.class.getName())) {
                RefAddr listenerName = ref.get("listenerName");
                RefAddr socketFactoryClass = ref.get("socketFactoryClass");
                RefAddr hostname = ref.get("hostname");
                RefAddr port = ref.get("port");
                RefAddr keepaliveInterval = ref.get("keepaliveInterval");
                RefAddr clientId = ref.get("clientId");
                RefAddr smqpProducerReplyInterval = ref.get("smqpProducerReplyInterval");
                RefAddr smqpConsumerCacheSize = ref.get("smqpConsumerCacheSize");
                RefAddr jmsDeliveryMode = ref.get("jmsDeliveryMode");
                RefAddr jmsPriority = ref.get("jmsPriority");
                RefAddr jmsTTL = ref.get("jmsTTL");
                RefAddr jmsMessageIdEnabled = ref.get("jmsMessageIdEnabled");
                RefAddr jmsMessageTimestampEnabled = ref.get("jmsMessageTimestampEnabled");
                RefAddr useThreadContextCL = ref.get("useThreadContextCL");
                RefAddr inputBufferSize = ref.get("inputBufferSize");
                RefAddr inputExtendSize = ref.get("inputExtendSize");
                RefAddr outputBufferSize = ref.get("outputBufferSize");
                RefAddr outputExtendSize = ref.get("outputExtendSize");
                RefAddr intraVM = ref.get("intraVM");
                RefAddr hostname2 = ref.get("hostname2");
                RefAddr port2 = ref.get("port2");
                RefAddr reconnectEnabled = ref.get("reconnectEnabled");
                RefAddr maxRetries = ref.get("maxRetries");
                RefAddr retryDelay = ref.get("retryDelay");
                RefAddr duplicateMessageDetection = ref.get("duplicateMessageDetection");
                RefAddr duplicateBacklogSize = ref.get("duplicateBacklogSize");
                if (listenerName != null &&
                        socketFactoryClass != null &&
                        hostname != null &&
                        port != null &&
                        keepaliveInterval != null &&
                        smqpProducerReplyInterval != null &&
                        smqpConsumerCacheSize != null &&
                        jmsDeliveryMode != null &&
                        jmsPriority != null &&
                        jmsTTL != null &&
                        jmsMessageIdEnabled != null &&
                        jmsMessageTimestampEnabled != null &&
                        useThreadContextCL != null &&
                        inputBufferSize != null &&
                        inputExtendSize != null &&
                        outputBufferSize != null &&
                        outputExtendSize != null &&
                        intraVM != null) {
                    com.swiftmq.jms.v610.ConnectionFactoryImpl cf = new com.swiftmq.jms.v610.ConnectionFactoryImpl((String) listenerName.getContent(),
                            (String) socketFactoryClass.getContent(),
                            (String) hostname.getContent(),
                            Integer.parseInt((String) port.getContent()),
                            Long.parseLong((String) keepaliveInterval.getContent()),
                            clientId == null ? null : (String) clientId.getContent(),
                            Integer.parseInt((String) smqpProducerReplyInterval.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSize.getContent()),
                            Integer.parseInt((String) jmsDeliveryMode.getContent()),
                            Integer.parseInt((String) jmsPriority.getContent()),
                            Long.parseLong((String) jmsTTL.getContent()),
                            Boolean.valueOf((String) jmsMessageIdEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) jmsMessageTimestampEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) useThreadContextCL.getContent()).booleanValue(),
                            Integer.parseInt((String) inputBufferSize.getContent()),
                            Integer.parseInt((String) inputExtendSize.getContent()),
                            Integer.parseInt((String) outputBufferSize.getContent()),
                            Integer.parseInt((String) outputExtendSize.getContent()),
                            Boolean.valueOf((String) intraVM.getContent()).booleanValue());
                    cf.setReconnectEnabled(Boolean.valueOf((String) reconnectEnabled.getContent()).booleanValue());
                    cf.setMaxRetries(Integer.valueOf((String) maxRetries.getContent()).intValue());
                    cf.setRetryDelay(Long.valueOf((String) retryDelay.getContent()).longValue());
                    cf.setDuplicateMessageDetection(Boolean.valueOf((String) duplicateMessageDetection.getContent()).booleanValue());
                    cf.setDuplicateBacklogSize(Integer.valueOf((String) duplicateBacklogSize.getContent()).intValue());

                    if (hostname2 != null) {
                        cf.setHostname2((String) hostname2.getContent());
                        cf.setPort2(Integer.parseInt((String) port2.getContent()));
                    }
                    return cf;
                }
            } else if (ref.getClassName().equals(com.swiftmq.jms.v630.ConnectionFactoryImpl.class.getName())) {
                RefAddr listenerName = ref.get("listenerName");
                RefAddr socketFactoryClass = ref.get("socketFactoryClass");
                RefAddr hostname = ref.get("hostname");
                RefAddr port = ref.get("port");
                RefAddr keepaliveInterval = ref.get("keepaliveInterval");
                RefAddr clientId = ref.get("clientId");
                RefAddr smqpProducerReplyInterval = ref.get("smqpProducerReplyInterval");
                RefAddr smqpConsumerCacheSize = ref.get("smqpConsumerCacheSize");
                RefAddr jmsDeliveryMode = ref.get("jmsDeliveryMode");
                RefAddr jmsPriority = ref.get("jmsPriority");
                RefAddr jmsTTL = ref.get("jmsTTL");
                RefAddr jmsMessageIdEnabled = ref.get("jmsMessageIdEnabled");
                RefAddr jmsMessageTimestampEnabled = ref.get("jmsMessageTimestampEnabled");
                RefAddr useThreadContextCL = ref.get("useThreadContextCL");
                RefAddr inputBufferSize = ref.get("inputBufferSize");
                RefAddr inputExtendSize = ref.get("inputExtendSize");
                RefAddr outputBufferSize = ref.get("outputBufferSize");
                RefAddr outputExtendSize = ref.get("outputExtendSize");
                RefAddr intraVM = ref.get("intraVM");
                RefAddr hostname2 = ref.get("hostname2");
                RefAddr port2 = ref.get("port2");
                RefAddr reconnectEnabled = ref.get("reconnectEnabled");
                RefAddr maxRetries = ref.get("maxRetries");
                RefAddr retryDelay = ref.get("retryDelay");
                RefAddr duplicateMessageDetection = ref.get("duplicateMessageDetection");
                RefAddr duplicateBacklogSize = ref.get("duplicateBacklogSize");
                if (listenerName != null &&
                        socketFactoryClass != null &&
                        hostname != null &&
                        port != null &&
                        keepaliveInterval != null &&
                        smqpProducerReplyInterval != null &&
                        smqpConsumerCacheSize != null &&
                        jmsDeliveryMode != null &&
                        jmsPriority != null &&
                        jmsTTL != null &&
                        jmsMessageIdEnabled != null &&
                        jmsMessageTimestampEnabled != null &&
                        useThreadContextCL != null &&
                        inputBufferSize != null &&
                        inputExtendSize != null &&
                        outputBufferSize != null &&
                        outputExtendSize != null &&
                        intraVM != null) {
                    com.swiftmq.jms.v630.ConnectionFactoryImpl cf = new com.swiftmq.jms.v630.ConnectionFactoryImpl((String) listenerName.getContent(),
                            (String) socketFactoryClass.getContent(),
                            (String) hostname.getContent(),
                            Integer.parseInt((String) port.getContent()),
                            Long.parseLong((String) keepaliveInterval.getContent()),
                            clientId == null ? null : (String) clientId.getContent(),
                            Integer.parseInt((String) smqpProducerReplyInterval.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSize.getContent()),
                            Integer.parseInt((String) jmsDeliveryMode.getContent()),
                            Integer.parseInt((String) jmsPriority.getContent()),
                            Long.parseLong((String) jmsTTL.getContent()),
                            Boolean.valueOf((String) jmsMessageIdEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) jmsMessageTimestampEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) useThreadContextCL.getContent()).booleanValue(),
                            Integer.parseInt((String) inputBufferSize.getContent()),
                            Integer.parseInt((String) inputExtendSize.getContent()),
                            Integer.parseInt((String) outputBufferSize.getContent()),
                            Integer.parseInt((String) outputExtendSize.getContent()),
                            Boolean.valueOf((String) intraVM.getContent()).booleanValue());
                    cf.setReconnectEnabled(Boolean.valueOf((String) reconnectEnabled.getContent()).booleanValue());
                    cf.setMaxRetries(Integer.valueOf((String) maxRetries.getContent()).intValue());
                    cf.setRetryDelay(Long.valueOf((String) retryDelay.getContent()).longValue());
                    cf.setDuplicateMessageDetection(Boolean.valueOf((String) duplicateMessageDetection.getContent()).booleanValue());
                    cf.setDuplicateBacklogSize(Integer.valueOf((String) duplicateBacklogSize.getContent()).intValue());

                    if (hostname2 != null) {
                        cf.setHostname2((String) hostname2.getContent());
                        cf.setPort2(Integer.parseInt((String) port2.getContent()));
                    }
                    return cf;
                }
            } else if (ref.getClassName().equals(com.swiftmq.jms.v750.ConnectionFactoryImpl.class.getName())) {
                RefAddr listenerName = ref.get("listenerName");
                RefAddr socketFactoryClass = ref.get("socketFactoryClass");
                RefAddr hostname = ref.get("hostname");
                RefAddr port = ref.get("port");
                RefAddr keepaliveInterval = ref.get("keepaliveInterval");
                RefAddr clientId = ref.get("clientId");
                RefAddr smqpProducerReplyInterval = ref.get("smqpProducerReplyInterval");
                RefAddr smqpConsumerCacheSize = ref.get("smqpConsumerCacheSize");
                RefAddr smqpConsumerCacheSizeKB = ref.get("smqpConsumerCacheSizeKB");
                RefAddr jmsDeliveryMode = ref.get("jmsDeliveryMode");
                RefAddr jmsPriority = ref.get("jmsPriority");
                RefAddr jmsTTL = ref.get("jmsTTL");
                RefAddr jmsMessageIdEnabled = ref.get("jmsMessageIdEnabled");
                RefAddr jmsMessageTimestampEnabled = ref.get("jmsMessageTimestampEnabled");
                RefAddr useThreadContextCL = ref.get("useThreadContextCL");
                RefAddr inputBufferSize = ref.get("inputBufferSize");
                RefAddr inputExtendSize = ref.get("inputExtendSize");
                RefAddr outputBufferSize = ref.get("outputBufferSize");
                RefAddr outputExtendSize = ref.get("outputExtendSize");
                RefAddr intraVM = ref.get("intraVM");
                RefAddr hostname2 = ref.get("hostname2");
                RefAddr port2 = ref.get("port2");
                RefAddr reconnectEnabled = ref.get("reconnectEnabled");
                RefAddr maxRetries = ref.get("maxRetries");
                RefAddr retryDelay = ref.get("retryDelay");
                RefAddr duplicateMessageDetection = ref.get("duplicateMessageDetection");
                RefAddr duplicateBacklogSize = ref.get("duplicateBacklogSize");
                if (listenerName != null &&
                        socketFactoryClass != null &&
                        hostname != null &&
                        port != null &&
                        keepaliveInterval != null &&
                        smqpProducerReplyInterval != null &&
                        smqpConsumerCacheSize != null &&
                        smqpConsumerCacheSizeKB != null &&
                        jmsDeliveryMode != null &&
                        jmsPriority != null &&
                        jmsTTL != null &&
                        jmsMessageIdEnabled != null &&
                        jmsMessageTimestampEnabled != null &&
                        useThreadContextCL != null &&
                        inputBufferSize != null &&
                        inputExtendSize != null &&
                        outputBufferSize != null &&
                        outputExtendSize != null &&
                        intraVM != null) {
                    com.swiftmq.jms.v750.ConnectionFactoryImpl cf = new com.swiftmq.jms.v750.ConnectionFactoryImpl((String) listenerName.getContent(),
                            (String) socketFactoryClass.getContent(),
                            (String) hostname.getContent(),
                            Integer.parseInt((String) port.getContent()),
                            Long.parseLong((String) keepaliveInterval.getContent()),
                            clientId == null ? null : (String) clientId.getContent(),
                            Integer.parseInt((String) smqpProducerReplyInterval.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSize.getContent()),
                            Integer.parseInt((String) smqpConsumerCacheSizeKB.getContent()),
                            Integer.parseInt((String) jmsDeliveryMode.getContent()),
                            Integer.parseInt((String) jmsPriority.getContent()),
                            Long.parseLong((String) jmsTTL.getContent()),
                            Boolean.valueOf((String) jmsMessageIdEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) jmsMessageTimestampEnabled.getContent()).booleanValue(),
                            Boolean.valueOf((String) useThreadContextCL.getContent()).booleanValue(),
                            Integer.parseInt((String) inputBufferSize.getContent()),
                            Integer.parseInt((String) inputExtendSize.getContent()),
                            Integer.parseInt((String) outputBufferSize.getContent()),
                            Integer.parseInt((String) outputExtendSize.getContent()),
                            Boolean.valueOf((String) intraVM.getContent()).booleanValue());
                    cf.setReconnectEnabled(Boolean.valueOf((String) reconnectEnabled.getContent()).booleanValue());
                    cf.setMaxRetries(Integer.valueOf((String) maxRetries.getContent()).intValue());
                    cf.setRetryDelay(Long.valueOf((String) retryDelay.getContent()).longValue());
                    cf.setDuplicateMessageDetection(Boolean.valueOf((String) duplicateMessageDetection.getContent()).booleanValue());
                    cf.setDuplicateBacklogSize(Integer.valueOf((String) duplicateBacklogSize.getContent()).intValue());

                    if (hostname2 != null) {
                        cf.setHostname2((String) hostname2.getContent());
                        cf.setPort2(Integer.parseInt((String) port2.getContent()));
                    }
                    return cf;
                }
            }


        }
        return null;
    }
}
