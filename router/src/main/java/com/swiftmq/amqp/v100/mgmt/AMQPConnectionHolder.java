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

package com.swiftmq.amqp.v100.mgmt;

import com.swiftmq.admin.mgmt.ConnectionHolder;
import com.swiftmq.admin.mgmt.Endpoint;
import com.swiftmq.admin.mgmt.ExceptionListener;
import com.swiftmq.admin.mgmt.RequestServiceFactory;
import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPBinary;
import com.swiftmq.jms.ReconnectListener;
import com.swiftmq.mgmt.protocol.ProtocolFactory;
import com.swiftmq.mgmt.protocol.ProtocolReply;
import com.swiftmq.mgmt.protocol.ProtocolRequest;
import com.swiftmq.net.JSSESocketFactory;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;
import com.swiftmq.tools.requestreply.TimeoutException;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class AMQPConnectionHolder implements ConnectionHolder {
    static final String MGMT_QUEUE = "swiftmqmgmt";
    static final int MGMT_PROTOCOL_VERSION = Integer.parseInt(System.getProperty("swiftmq.mgmt.protocol.version", "750"));
    static final String Q_MECHANISM = "mechanism";
    static final String Q_MAXFRAMESIZE = "maxframesize";
    static final String Q_IDLETIMEOUT = "idletimeout";

    Connection connection = null;
    String urlString = null;

    public AMQPConnectionHolder(Connection connection) {
        this.connection = connection;
    }

    public AMQPConnectionHolder(String urlString) {
        this.urlString = urlString;
    }

    private Map<String, String> getQueryMap(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params) {
            String name = param.split("=")[0];
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }

    public void connect(String username, String password) throws Exception {
        // parse url
        URL url = new URL(urlString.replaceFirst("amqp:", "http:").replaceFirst("amqps:", "https:"));
        String hostname = url.getHost();
        if (hostname == null)
            hostname = "localhost";
        int port = url.getPort();
        if (port == -1)
            port = urlString.startsWith("amqps:") ? 5671 : 5672;

        // create connection
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        if (username == null || username.trim().length() == 0)
            connection = new Connection(ctx, hostname, port, true);
        else
            connection = new Connection(ctx, hostname, port, username, password);
        if (urlString.startsWith("amqps:"))
            connection.setSocketFactory(new JSSESocketFactory());
        String query = url.getQuery();
        if (query != null) {
            Map<String, String> queryMap = getQueryMap(query);
            String v = queryMap.get(Q_MECHANISM);
            if (v != null)
                connection.setMechanism(v);
            v = queryMap.get(Q_MAXFRAMESIZE);
            if (v != null)
                connection.setMaxFrameSize(Long.parseLong(v));
            v = queryMap.get(Q_IDLETIMEOUT);
            if (v != null)
                connection.setIdleTimeout(Long.parseLong(v));

        }
        connection.connect();
    }

    public void start() throws Exception {
    }

    public void setExceptionListener(final ExceptionListener exceptionListener) throws Exception {
        connection.setExceptionListener(new com.swiftmq.amqp.v100.client.ExceptionListener() {
            public void onException(Exception exception) {
                try {
                    exceptionListener.onException(exception);
                } catch (Exception e) {
                }
            }
        });
    }

    public void addReconnectListener(ReconnectListener reconnectListener) {
        // NOOP
    }

    public void removeReconnectListener(ReconnectListener reconnectListener) {
        // NOOP
    }

    public Endpoint createEndpoint(String routerName, RequestServiceFactory requestServiceFactory, boolean createInternalCommands) throws Exception {
        return new EndpointCreator(routerName, connection).create(requestServiceFactory, createInternalCommands);
    }

    public void close() {
        if (connection != null)
            connection.close();
    }

    private static class EndpointCreator {
        String routerName = null;
        Connection connection = null;
        Session session = null;
        Producer sender = null;
        Consumer receiver = null;
        AddressIF replyAddress = null;

        private EndpointCreator(String routerName, Connection connection) throws Exception {
            this.routerName = routerName;
            this.connection = connection;
            String queueName = null;
            if (routerName == null)
                queueName = MGMT_QUEUE;
            else
                queueName = MGMT_QUEUE + "@" + routerName;
            try {
                session = connection.createSession(100, 100);
                sender = session.createProducer(queueName, QoS.AT_MOST_ONCE);
                receiver = session.createConsumer(100, QoS.AT_MOST_ONCE);
                replyAddress = receiver.getRemoteAddress();
            } catch (Exception e) {
                cleanup();
                throw e;
            }
        }

        private AMQPMessage requestToMessage(Request request) throws Exception {
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
            dos.rewind();
            Dumpalizer.dump(dos, request);
            AMQPMessage msg = new AMQPMessage();
            byte[] bytes = new byte[dos.getCount()];
            System.arraycopy(dos.getBuffer(), 0, bytes, 0, bytes.length);
            msg.addData(new Data(bytes));
            return msg;
        }

        private Reply messageToReply(AMQPMessage msg) throws Exception {
            Data data = msg.getData().get(0);
            DataByteArrayInputStream dis = new DataByteArrayInputStream(data.getValue());
            return (Reply) Dumpalizer.construct(dis, new ProtocolFactory());
        }

        private Reply request(Request request) throws Exception {
            AMQPMessage msg = requestToMessage(request);
            Properties prop = new Properties();
            if (connection.getUserName() == null)
                prop.setUserId(new AMQPBinary("anonymous".getBytes()));
            else
                prop.setUserId(new AMQPBinary(connection.getUserName().getBytes()));
            prop.setReplyTo(replyAddress);
            msg.setProperties(prop);
            sender.send(msg);
            msg = receiver.receive(RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT);
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
                            endpoint = new com.swiftmq.amqp.v100.mgmt.v750.EndpointImpl(connection, session, sender, receiver, replyAddress, rsf.createRequestService(750), createInternalCommands);
                            endpoint.setSubscriptionFilterEnabled(true);
                        } else {
                            pr = (ProtocolReply) request(new ProtocolRequest(400));
                            if (!pr.isOk())
                                throw pr.getException();
                            endpoint = new com.swiftmq.amqp.v100.mgmt.v400.EndpointImpl(connection, session, sender, receiver, replyAddress, rsf.createRequestService(400), createInternalCommands);
                        }
                    }
                    break;
                    case 400: {
                        ProtocolReply pr = (ProtocolReply) request(new ProtocolRequest(400));
                        if (!pr.isOk())
                            throw pr.getException();
                        endpoint = new com.swiftmq.amqp.v100.mgmt.v400.EndpointImpl(connection, session, sender, receiver, replyAddress, rsf.createRequestService(400), createInternalCommands);
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
            if (session != null) {
                try {
                    session.close();
                } catch (Exception e) {
                }
            }
        }
    }
}
