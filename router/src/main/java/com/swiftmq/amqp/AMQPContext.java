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

package com.swiftmq.amqp;

import com.swiftmq.amqp.integration.ClientTracer;
import com.swiftmq.amqp.integration.Tracer;
import com.swiftmq.client.thread.PoolManager;
import com.swiftmq.swiftlet.threadpool.ThreadPool;

/**
 * <p>
 * AMQP context provides thread pools and trace objects to the SwiftMQ client.
 * </p>
 * <p>
 * It is passed to the AMQP connection
 * and must be instantiated as client context: <code>new AMQPContext(AMQPContext.CLIENT);</code>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPContext {
    public static final int CLIENT = 0;
    public static final int ROUTER = 1;

    int ctx;
    Tracer frameTracer = null;
    Tracer processingTracer = null;
    ThreadPool connectionPool = null;
    ThreadPool sessionPool = null;

    /**
     * Constructs an AMQP context.
     *
     * @param ctx the context, CLIENT or ROUTER
     */
    public AMQPContext(int ctx) {
        this.ctx = ctx;
        if (ctx == CLIENT)
            PoolManager.setIntraVM(false);
    }

    /**
     * Returns a frame tracer.
     *
     * @return frame tracer
     */
    public synchronized Tracer getFrameTracer() {
        if (frameTracer == null) {
            if (ctx == CLIENT)
                frameTracer = new ClientTracer("swiftmq.amqp.frame.debug");
            else
                frameTracer = getRouterFrameTracer();
        }
        return frameTracer;
    }

    /**
     * Returns a processing tracer.
     *
     * @return processing tracer
     */
    public synchronized Tracer getProcessingTracer() {
        if (processingTracer == null) {
            if (ctx == CLIENT)
                processingTracer = new ClientTracer("swiftmq.amqp.debug");
            else
                processingTracer = getRouterProcessiongTracer();
        }
        return processingTracer;
    }

    /**
     * Returns the connection thread pool.
     *
     * @return connection thread pool
     */
    public synchronized ThreadPool getConnectionPool() {
        if (connectionPool == null) {
            if (ctx == CLIENT)
                connectionPool = PoolManager.getInstance().getConnectionPool();
            else
                connectionPool = getRouterConnectionPool();
        }
        return connectionPool;
    }

    /**
     * Returns the session thread pool.
     *
     * @return session thread pool
     */
    public synchronized ThreadPool getSessionPool() {
        if (sessionPool == null) {
            if (ctx == CLIENT)
                sessionPool = PoolManager.getInstance().getSessionPool();
            else
                sessionPool = getRouterSessionPool();
        }
        return sessionPool;
    }

    protected Tracer getRouterFrameTracer() {
        return null;
    }

    protected Tracer getRouterProcessiongTracer() {
        return null;
    }

    protected ThreadPool getRouterConnectionPool() {
        return null;
    }

    protected ThreadPool getRouterSessionPool() {
        return null;
    }
}
