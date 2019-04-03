package com.swiftmq.jms.springsupport;

import com.swiftmq.admin.cli.CLI;

import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

public class CLIHelper {
    QueueConnectionFactory connectionFactory = null;
    QueueConnection connection = null;
    CLI cli = null;
    String userName = null;
    String password = null;
    String router = null;
    String[] initCommands = null;
    String[] destroyCommands = null;

    public QueueConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(QueueConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRouter() {
        return router;
    }

    public void setRouter(String router) {
        this.router = router;
    }

    public String[] getInitCommands() {
        return initCommands;
    }

    public void setInitCommands(String[] initCommands) {
        this.initCommands = initCommands;
    }

    public String[] getDestroyCommands() {
        return destroyCommands;
    }

    public void setDestroyCommands(String[] destroyCommands) {
        this.destroyCommands = destroyCommands;
    }

    public void initialize() throws Exception {
        if (connectionFactory == null)
            throw new Exception("connectionFactory has not been set");
        if (router == null)
            throw new Exception("router not specified");
        if (initCommands == null)
            throw new Exception("No init commands specified");
        connection = connectionFactory.createQueueConnection(userName, password);
        cli = new CLI(connection);
        cli.waitForRouter(router);
        cli.executeCommand("sr " + router);
        for (int i = 0; i < initCommands.length; i++) {
            cli.executeCommand(initCommands[i]);
        }
    }

    public void destroy() throws Exception {
        if (connectionFactory == null)
            throw new Exception("connectionFactory has not been set");
        if (router == null)
            throw new Exception("router not specified");
        if (destroyCommands == null)
            throw new Exception("No destroy commands specified");
        if (connection == null) {
            connection = connectionFactory.createQueueConnection(userName, password);
            cli = new CLI(connection);
        }
        cli.executeCommand("sr " + router);
        cli.waitForRouter(router);
        for (int i = 0; i < destroyCommands.length; i++) {
            cli.executeCommand(destroyCommands[i]);
        }
        cli.close();
        connection.close();
        cli = null;
        connection = null;
    }
}
