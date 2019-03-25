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

package com.swiftmq.swiftlet.accounting;

import com.swiftmq.swiftlet.Swiftlet;

/**
 * The Accounting Swiftlet is the interface for accounting (usage data) for other Swiftlets. Hereto other Swiftlets
 * register their Accounting Source and/or Sink Factories from which the user can create accounting connections.
 * <p>
 * The factories are registered under a group and factory name. The group name should be the clear name of a Swiftlet like "JMS".
 * The factory name should be the clear name of the factory like "QueueSinkFactoty".
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2010, All Rights Reserved
 */
public abstract class AccountingSwiftlet extends Swiftlet {
    /**
     * Add a new AccountingSourceFactory.
     *
     * @param group   group name
     * @param name    factory name
     * @param factory factory
     */
    public abstract void addAccountingSourceFactory(String group, String name, AccountingSourceFactory factory);

    /**
     * Removes a AccountingSourceFactory.
     *
     * @param group group name
     * @param name  factory name
     */
    public abstract void removeAccountingSourceFactory(String group, String name);

    /**
     * Add a new AccountingSinkFactory.
     *
     * @param group   group name
     * @param name    factory name
     * @param factory factory
     */
    public abstract void addAccountingSinkFactory(String group, String name, AccountingSinkFactory factory);

    /**
     * Removes a AccountingSinkFactory.
     *
     * @param group group name
     * @param name  factory name
     */
    public abstract void removeAccountingSinkFactory(String group, String name);
}
