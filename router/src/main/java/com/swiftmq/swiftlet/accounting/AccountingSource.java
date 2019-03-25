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

/**
 * AccountingSource specifies the interface for classes where accounting messages originate from.
 * It is started with startAccounting and continuously adds accounting messages to the AccountingSink
 * via its add method until stopAccounting is called or an error occurs in which case the StopListener
 * is called if set.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2010, All Rights Reserved
 */
public interface AccountingSource {
    /**
     * Sets a StopListener which will be called if the account source is stopped internally
     * due to an exception.
     *
     * @param listener StopListener
     */
    public void setStopListener(StopListener listener);

    /**
     * Starts accounting
     *
     * @param sink The accounting sink
     * @throws Exception if anything goes wrong
     */
    public void startAccounting(AccountingSink sink) throws Exception;

    /**
     * Stops the accounting. The StopListener will NOT be called.
     *
     * @throws Exception if anything goes wrong
     */
    public void stopAccounting() throws Exception;
}
