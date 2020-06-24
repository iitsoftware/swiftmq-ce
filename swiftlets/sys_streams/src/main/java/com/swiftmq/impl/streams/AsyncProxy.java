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

import java.lang.reflect.Method;

/**
 * This class can be used to wrap any async callback in external Java libs, that are used
 * from a Stream, to execute the callback on the event loop of the Stream.
 */
public class AsyncProxy implements java.lang.reflect.InvocationHandler {

    private Stream stream;
    private Object obj;

    private AsyncProxy(Stream stream, Object obj) {
        this.stream = stream;
        this.obj = obj;
    }

    public static Object newInstance(Stream stream, Object obj) {
        return java.lang.reflect.Proxy.newProxyInstance(
                obj.getClass().getClassLoader(),
                obj.getClass().getInterfaces(),
                new AsyncProxy(stream, obj));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        stream.executeCallback(new FunctionCallback() {
            @Override
            public void execute(Object context) {
                try {
                    method.invoke(obj, args);
                } catch (Exception e) {
                    stream.getStreamCtx().logStackTrace(e);
                }
            }
        }, null);
        return null;
    }
}
