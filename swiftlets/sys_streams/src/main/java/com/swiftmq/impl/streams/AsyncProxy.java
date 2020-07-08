/*
 * Copyright 2020 IIT Software GmbH
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

package com.swiftmq.impl.streams;

import com.swiftmq.impl.streams.processor.po.POExecute;

import java.lang.reflect.Method;

/**
 * Proxy to intercept asynchronous callbacks and executes them on the stream's event queue
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2020, All Rights Reserved
 */
public class AsyncProxy implements java.lang.reflect.InvocationHandler {

    private Object obj;
    private Stream stream;
    private int hashcode;
    private String toString;

    private AsyncProxy(Stream stream, Object obj) {
        this.stream = stream;
        this.obj = obj;
        this.hashcode = obj.hashCode();
        this.toString = obj.toString();
    }

    public static Object newInstance(Stream stream, String interfaceClassName, Object obj) throws Exception {
        return java.lang.reflect.Proxy.newProxyInstance(
                stream.getStreamCtx().classLoader,
                new Class[]{stream.getStreamCtx().classLoader.loadClass(interfaceClassName)},
                new AsyncProxy(stream, obj));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        stream.getStreamCtx().streamProcessor.dispatch(new POExecute(null, () -> {
            try {
                method.invoke(obj, args);
            } catch (Exception e) {
                stream.getStreamCtx().logStackTrace(e);
            }
        }));
        return null;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        return obj.equals(this.obj);
    }

    @Override
    public String toString() {
        return toString;
    }
}
