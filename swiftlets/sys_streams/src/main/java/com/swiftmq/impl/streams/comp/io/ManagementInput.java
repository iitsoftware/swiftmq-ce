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

package com.swiftmq.impl.streams.comp.io;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.impl.streams.processor.ManagementProcessor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.util.SwiftUtilities;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes Messages from the Management Tree.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class ManagementInput implements Input {
    public static String PROP_OPER = "_OP";
    public static String PROP_CTX = "_CTX";
    public static String PROP_TIME = "_TIME";
    public static String VAL_ADD = "ADD";
    public static String VAL_REMOVE = "REMOVE";
    public static String VAL_CHANGE = "CHANGE";

    StreamContext ctx;
    String name;
    String context = null;
    Message current;
    InputCallback addCallback;
    InputCallback removeCallback;
    InputCallback changeCallback;
    String selector;
    int msgsProcessed = 0;
    int totalMsg = 0;
    Entity usage = null;
    ManagementProcessor managementProcessor;
    boolean started = false;
    List<String> propIncludes;

    ManagementInput(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        this.context = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Internal use
     *
     * @return true/false
     */
    public boolean hasAddCallback() {
        return addCallback != null;
    }


    /**
     * Internal use
     *
     * @return true/false
     */
    public boolean hasRemoveCallback() {
        return removeCallback != null;
    }


    /**
     * Internal use
     *
     * @return true/false
     */
    public boolean hasChangeCallback() {
        return changeCallback != null;
    }

    @Override
    public Input current(Message current) {
        this.current = current;
        return this;
    }

    @Override
    public Message current() {
        return current;
    }

    /**
     * Internal use only.
     */
    public String getSelector() {
        return selector;
    }

    /**
     * Sets a CLI context different from the name.
     * @param context  CLI context
     * @return this
     */
    public Input context(String context) {
        this.context = context;
        return this;
    }

    /**
     * Returns the CLI context
     * @return CLI context
     */
    public String context() {
        return context;
    }
    /**
     * Sets a JMS Message selector
     *
     * @param selector JMS Message Selector
     * @return this
     */
    public Input selector(String selector) {
        this.selector = selector;
        return this;
    }

    /**
     * Adds Property name(s) that must be addionally included in an onChange message.
     * If multiple names are given, they must be separated by blank.
     *
     * @param propInclude
     * @return this
     */
    public ManagementInput include(String propInclude) {
        if (propIncludes == null)
            propIncludes = new ArrayList<String>();
        String s[] = SwiftUtilities.tokenize(propInclude, " ");
        for (int i = 0; i < s.length; i++)
            propIncludes.add(s[i].replace('_', '-'));
        return this;
    }

    /**
     * Internal use only.
     */
    public List<String> getPropIncludes() {
        return propIncludes;
    }

    /**
     * Registers a Callback that is executed when a new Entity is added
     *
     * @param addCallback callback
     * @return ManagementInput
     */
    public ManagementInput onAdd(InputCallback addCallback) {
        this.addCallback = addCallback;
        return this;
    }

    /**
     * Registers a Callback that is executed when a existing Entity is removed
     *
     * @param removeCallback callback
     * @return ManagementInput
     */
    public ManagementInput onRemove(InputCallback removeCallback) {
        this.removeCallback = removeCallback;
        return this;
    }

    /**
     * Registers a Callback that is executed when a a Property of an Entity has changed
     *
     * @param changeCallback callback
     * @return ManagementInput
     */
    public ManagementInput onChange(InputCallback changeCallback) {
        this.changeCallback = changeCallback;
        return this;
    }

    @Override
    public void executeCallback() throws Exception {
        String op = current.property(PROP_OPER).value().toString();
        if (op == null)
            throw new NullPointerException("Missing property in message: " + PROP_OPER);
        if (op.equals(VAL_ADD)) {
            if (addCallback != null)
                addCallback.execute(this);
        } else if (op.equals(VAL_REMOVE)) {
            if (removeCallback != null)
                removeCallback.execute(this);
        } else if (op.equals(VAL_CHANGE)) {
            if (changeCallback != null)
                changeCallback.execute(this);
        }
        msgsProcessed++;
    }

    /**
     * Internal use.
     *
     * @return ManagementProcessor
     */
    public ManagementProcessor getManagementProcessor() {
        return managementProcessor;
    }

    /**
     * Internal use.
     *
     * @param managementProcessor ManagementProcessor
     */
    public void setManagementProcessor(ManagementProcessor managementProcessor) {
        this.managementProcessor = managementProcessor;
    }

    @Override
    public void collect(long interval) {
        if (!started)
            return;
        if ((long) totalMsg + (long) msgsProcessed > Integer.MAX_VALUE)
            totalMsg = 0;
        totalMsg += msgsProcessed;
        try {
            usage.getProperty("input-total-processed").setValue(totalMsg);
            usage.getProperty("input-processing-rate").setValue((int) (msgsProcessed / ((double) interval / 1000.0)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        msgsProcessed = 0;
    }

    @Override
    public void start() throws Exception {
        if (started)
            return;
        try {
            EntityList inputList = (EntityList) ctx.usage.getEntity("inputs");
            usage = inputList.createEntity();
            usage.setName(name.replace('/', '_'));
            usage.createCommands();
            inputList.addEntity(usage);
            usage.getProperty("01-type").setValue("Management");
        } catch (Exception e) {
            e.printStackTrace();
        }
        managementProcessor = new ManagementProcessor(ctx, this);
        managementProcessor.register();
        started = true;
    }

    @Override
    public void close() {
        if (started) {
            try {
                if (usage != null)
                    ctx.usage.getEntity("inputs").removeEntity(usage);
            } catch (EntityRemoveException e) {
            }
            managementProcessor.deregister();
            managementProcessor = null;
            ctx.stream.removeInput(this);
            started = false;
        }
    }

    @Override
    public String toString() {
        return "ManagementInput{" +
                "name='" + name + '\'' +
                '}';
    }
}
