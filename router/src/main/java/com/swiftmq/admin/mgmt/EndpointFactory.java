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

package com.swiftmq.admin.mgmt;

import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.mgmt.protocol.ProtocolFactory;
import com.swiftmq.mgmt.protocol.ProtocolReply;
import com.swiftmq.mgmt.protocol.ProtocolRequest;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;
import com.swiftmq.tools.requestreply.TimeoutException;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.*;

public class EndpointFactory {
    static final String MGMT_QUEUE = "swiftmqmgmt";
    static final int MGMT_PROTOCOL_VERSION = Integer.parseInt(System.getProperty("swiftmq.mgmt.protocol.version", "750"));

    public static Endpoint createEndpoint(String routerName, QueueConnection connection, RequestServiceFactory rsf, boolean createInternalCommands) throws Exception {
        return new EndpointCreator(routerName, connection).create(rsf, createInternalCommands);
    }

    private static class EndpointCreator {
        String routerName = null;
        QueueConnection connection = null;
        QueueSession senderSession = null;
        QueueSender sender = null;
        QueueSession receiverSession = null;
        QueueReceiver receiver = null;
        TemporaryQueue replyQueue = null;

        private EndpointCreator(String routerName, QueueConnection connection) throws Exception {
            this.routerName = routerName;
            this.connection = connection;
            String queueName = null;
            if (routerName == null)
                queueName = MGMT_QUEUE;
            else
                queueName = MGMT_QUEUE + "@" + routerName;
            try {
                senderSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                sender = senderSession.createSender(new QueueImpl(queueName));
                sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                sender.setPriority(MessageImpl.MAX_PRIORITY - 1);
                receiverSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                replyQueue = receiverSession.createTemporaryQueue();
                receiver = receiverSession.createReceiver(replyQueue);
            } catch (JMSException e) {
                cleanup();
                throw e;
            }
        }

        private BytesMessageImpl requestToMessage(Request request) throws Exception {
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
            dos.rewind();
            Dumpalizer.dump(dos, request);
            BytesMessageImpl msg = new BytesMessageImpl();
            msg.writeBytes(dos.getBuffer(), 0, dos.getCount());
            return msg;
        }

        private Reply messageToReply(BytesMessageImpl msg) throws Exception {
            int len = (int) msg._getBodyLength();
            byte[] buffer = new byte[len];
            msg.readBytes(buffer);
            DataByteArrayInputStream dis = new DataByteArrayInputStream(buffer);
            return (Reply) Dumpalizer.construct(dis, new ProtocolFactory());
        }

        private Reply request(Request request) throws Exception {
            BytesMessageImpl msg = requestToMessage(request);
            msg.setJMSReplyTo(replyQueue);
            sender.send(msg);
            msg = (BytesMessageImpl) receiver.receive(RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT);
            if (msg == null)
                throw new TimeoutException("Request timeout occured (" + RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT + ") ms");
            return messageToReply(msg);
        }

        public Endpoint create(RequestServiceFactory rsf, boolean createInternalCommands) throws Exception {

            Endpoint endpoint = null;
            try {
                switch (MGMT_PROTOCOL_VERSION) {
                    case 750: {
                        ProtocolReply pr = (ProtocolReply) request(new ProtocolRequest(750));
                        if (pr.isOk()) {
                            endpoint = new com.swiftmq.admin.mgmt.v750.EndpointImpl(connection, senderSession, sender, receiverSession, receiver, replyQueue, rsf.createRequestService(750), createInternalCommands);
                            endpoint.setSubscriptionFilterEnabled(true);
                        } else {
                            pr = (ProtocolReply) request(new ProtocolRequest(400));
                            if (!pr.isOk())
                                throw pr.getException();
                            endpoint = new com.swiftmq.admin.mgmt.v400.EndpointImpl(connection, senderSession, sender, receiverSession, receiver, replyQueue, rsf.createRequestService(400), createInternalCommands);
                        }
                    }
                    break;
                    case 400: {
                        ProtocolReply pr = (ProtocolReply) request(new ProtocolRequest(400));
                        if (!pr.isOk())
                            throw pr.getException();
                        endpoint = new com.swiftmq.admin.mgmt.v400.EndpointImpl(connection, senderSession, sender, receiverSession, receiver, replyQueue, rsf.createRequestService(400), createInternalCommands);
                    }
                    break;
                    default:
                        throw new Exception("Invalid management protocol version (set via swiftmq.mgmt.protocol.version): " + MGMT_PROTOCOL_VERSION);
                }
            } catch (Exception e) {
                cleanup();
                throw e;
            }
            return endpoint;
        }

        private void cleanup() {
            if (senderSession != null) {
                try {
                    senderSession.close();
                } catch (JMSException e) {
                }
            }
            if (receiverSession != null) {
                try {
                    receiverSession.close();
                } catch (JMSException e) {
                }
            }
        }
    }
}
