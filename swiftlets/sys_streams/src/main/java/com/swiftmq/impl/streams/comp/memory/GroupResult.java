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

package com.swiftmq.impl.streams.comp.memory;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.comp.message.Message;

/**
 * Represents the result of a group operation on a Memory. It helds an array of Memories.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class GroupResult {
    StreamContext ctx;

    String groupPropName;
    Memory[] result;

    GroupResult(StreamContext ctx, String groupPropName, Memory[] result) {
        this.ctx = ctx;
        this.groupPropName = groupPropName;
        this.result = result;
    }

    /**
     * Returns the result as an array of Memories.
     *
     * @return Array of Memories
     */
    public Memory[] result() {
        return result;
    }

    /**
     * Determines the minimum value of a Property and returns the result as a new non-queue Memory with
     * Messages that contains 2 Properties, the grouping Property and the minimum Property.
     *
     * @param propName Property Name
     * @return Result
     * @throws Exception
     */
    public Memory min(String propName) throws Exception {
        Memory child = new HeapMemory(ctx);
        for (int i = 0; i < result.length; i++) {
            Message message = result[i].first();
            child.add(ctx.messageBuilder.message().property(groupPropName).set(message.property(groupPropName).value().toObject()).
                    property(propName).set(result[i].min(propName)));
        }
        return child;
    }

    /**
     * Determines the maximum value of a Property and returns the result as a new non-queue Memory with
     * Messages that contains 2 Properties, the grouping Property and the maximum Property.
     *
     * @param propName Property Name
     * @return Result
     * @throws Exception
     */
    public Memory max(String propName) throws Exception {
        Memory child = new HeapMemory(ctx);
        for (int i = 0; i < result.length; i++) {
            Message message = result[i].first();
            child.add(ctx.messageBuilder.message().property(groupPropName).set(message.property(groupPropName).value().toObject()).
                    property(propName).set(result[i].max(propName)));
        }
        return child;
    }

    /**
     * Determines the sum of all values of a Property and returns the result as a new non-queue Memory with
     * Messages that contains 2 Properties, the grouping Property and the sum Property.
     *
     * @param propName Property Name
     * @return Result
     * @throws Exception
     */
    public Memory sum(String propName) throws Exception {
        Memory child = new HeapMemory(ctx);
        for (int i = 0; i < result.length; i++) {
            Message message = result[i].first();
            child.add(ctx.messageBuilder.message().property(groupPropName).set(message.property(groupPropName).value().toObject()).
                    property(propName).set(result[i].sum(propName)));
        }
        return child;
    }

    /**
     * Determines the average value of a Property and returns the result as a new non-queue Memory with
     * Messages that contains 2 Properties, the grouping Property and the average Property.
     *
     * @param propName Property Name
     * @return Result
     * @throws Exception
     */
    public Memory average(String propName) throws Exception {
        Memory child = new HeapMemory(ctx);
        for (int i = 0; i < result.length; i++) {
            Message message = result[i].first();
            child.add(ctx.messageBuilder.message().property(groupPropName).set(message.property(groupPropName).value().toObject()).
                    property(propName).set(result[i].average(propName)));
        }
        return child;
    }

    /**
     * Executes the Callback for each Memory in this GroupResult
     *
     * @param callback Callback
     * @throws Exception
     */
    public void forEach(ForEachMemoryCallback callback) throws Exception {
        for (int i = 0; i < result.length; i++) {
            callback.execute(result[i]);
        }
    }
}
