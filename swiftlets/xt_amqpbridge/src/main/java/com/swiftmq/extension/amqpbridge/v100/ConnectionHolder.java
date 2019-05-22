package com.swiftmq.extension.amqpbridge.v100;

import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.extension.amqpbridge.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.net.JSSESocketFactory;

public class ConnectionHolder {
    SwiftletContext ctx = null;
    Entity connectionEntity = null;
    Connection connection = null;
    Session session = null;
    Producer producer = null;
    Consumer consumer = null;
    String remoteHost = null;
    String address = null;
    int remotePort = 0;
    int qos;

    public ConnectionHolder(SwiftletContext ctx, Entity connectionEntity, int qos) {
        this.ctx = ctx;
        this.connectionEntity = connectionEntity;
        this.qos = qos;
        remoteHost = (String) connectionEntity.getProperty("remote-hostname").getValue();
        remotePort = (Integer) connectionEntity.getProperty("remote-port").getValue();
    }

    public Producer getProducer() {
        return producer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void createConnection(ExceptionListener exceptionListener, DeliveryMemory deliveryMemory) throws Exception {
        // Create the connection
        String tplName = (String) connectionEntity.getProperty("connection-template").getValue();
        Entity connectionTemplate = ctx.connectionTemplates.getEntity(tplName);
        if (connectionTemplate == null) {
            if (tplName.equals("default"))
                connectionTemplate = ctx.connectionTemplates.getTemplate();
            else
                throw new Exception("Connection template '" + tplName + "' not found!");
        }

        boolean doAuth = (Boolean) connectionEntity.getProperty("sasl-enabled").getValue();
        boolean anon = (Boolean) connectionEntity.getProperty("sasl-anonymous-login").getValue();

        if (!doAuth || anon)
            connection = new Connection(ctx.amqpContext, remoteHost, remotePort, doAuth);
        else {
            String username = (String) connectionEntity.getProperty("sasl-loginname").getValue();
            String password = (String) connectionEntity.getProperty("sasl-password").getValue();
            if (username == null || username.trim().length() == 0)
                throw new Exception("sasl-loginname must be set when using SASL");
            if (password == null || password.trim().length() == 0)
                throw new Exception("sasl-password must be set when using SASL");
            connection = new Connection(ctx.amqpContext, remoteHost, remotePort, username, password);
        }
        if (exceptionListener != null)
            connection.setExceptionListener(exceptionListener);
        connection.setMechanism((String) connectionEntity.getProperty("sasl-mechanism").getValue());
        connection.setIdleTimeout((Long) connectionTemplate.getProperty("idle-timeout").getValue());
        connection.setMaxFrameSize((Long) connectionTemplate.getProperty("max-frame-size").getValue());
        String containerId = (String) connectionTemplate.getProperty("container-id").getValue();
        if (containerId != null && containerId.trim().length() > 0)
            connection.setContainerId(containerId);
        String openHostname = (String) connectionTemplate.getProperty("open-hostname").getValue();
        if (openHostname != null && openHostname.trim().length() > 0)
            connection.setOpenHostname(openHostname);
        boolean useSSL = (Boolean) connectionEntity.getProperty("use-ssl").getValue();
        if (useSSL)
            connection.setSocketFactory(new JSSESocketFactory());
        connection.setInputBufferSize((Integer) connectionTemplate.getProperty("input-buffer-size").getValue());
        connection.setInputBufferExtendSize((Integer) connectionTemplate.getProperty("input-buffer-extend-size").getValue());
        connection.setOutputBufferSize((Integer) connectionTemplate.getProperty("output-buffer-size").getValue());
        connection.setOutputBufferExtendSize((Integer) connectionTemplate.getProperty("output-buffer-extend-size").getValue());
        connection.connect();

        // Create sessions and links
        session = connection.createSession((Integer) connectionTemplate.getProperty("session-incoming-window-size").getValue(),
                (Integer) connectionTemplate.getProperty("session-outgoing-window-size").getValue());

        if (connectionEntity.getProperty("source-address") != null) {
            String selector = (String) connectionEntity.getProperty("source-message-selector").getValue();
            if (selector == null || selector.trim().length() == 0)
                selector = null;
            address = (String) connectionEntity.getProperty("source-address").getValue();
            consumer = session.createConsumer(address, (Integer) connectionTemplate.getProperty("source-link-credit").getValue(), qos, false, selector);
        } else {
            address = (String) connectionEntity.getProperty("target-address").getValue();
            producer = session.createProducer(address, qos, deliveryMemory);
        }
    }

    public void cancel() {
        if (connection != null) {
            connection.cancel();
            connection = null;
            producer = null;
            consumer = null;
            session = null;
        }
    }

    public void close() {
        if (connection != null) {
            connection.close();
            connection = null;
            producer = null;
            consumer = null;
            session = null;
        }
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("[ConnectionHolder");
        sb.append(" remoteHost='").append(remoteHost).append('\'');
        sb.append(", remotePort=").append(remotePort);
        sb.append(", address=").append(address);
        sb.append(']');
        return sb.toString();
    }
}
