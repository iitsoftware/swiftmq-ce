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

package com.swiftmq.swiftlet.jndi;

import com.swiftmq.swiftlet.Swiftlet;

import java.io.Serializable;

/**
 * The JNDI Swiftlet provides an interface to the JNDI subsystem of SwiftMQ. It is
 * implementation depending whether this points to an internal or external JNDI.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class JNDISwiftlet extends Swiftlet {
    public static final String JNDI_TOPIC = "swiftmq.jndi";
    public static final String JNDI_QUEUE = "sys$jndi";

    /**
     * Registers a JNDI object.
     * After registration, clients are able to lookup this object unter that name.
     *
     * @param name   lookup name.
     * @param object the object.
     */
    public abstract void registerJNDIObject(String name, Serializable object);


    /**
     * Removes a registered JNDI object.
     *
     * @param name lookup name.
     */
    public abstract void deregisterJNDIObject(String name);

    /**
     * Removes all registered JNDI objects that matches the comparable.
     *
     * @param comparable comparable.
     */
    public abstract void deregisterJNDIObjects(Comparable comparable);


    /**
     * Removes all JNDI entries for a registered JMS Queue.
     * Say, there are 3 registrations in JNDI which are all pointing to the same queue name
     * 'testqueue@router1', all registrations are deleted after calling this method with
     * 'testqueue@router1'.
     *
     * @param queueName queue name.
     */
    public abstract void deregisterJNDIQueueObject(String queueName);

    /**
     * Returns the JNDI Object with that name.
     *
     * @param name Name
     * @return Object
     */
    public abstract Serializable getJNDIObject(String name);

}

