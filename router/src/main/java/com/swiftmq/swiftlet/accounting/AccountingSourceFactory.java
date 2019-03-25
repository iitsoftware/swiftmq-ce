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

import java.util.Map;

/**
 * An AccountingSourceFactory specifies the interface for classes which creates
 * AccountingSourses.
 * <p>
 * An AccountingSourceFactory always belongs to a group (usually a Swiftlet) and
 * a name (usually the class name of the factory).
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2010, All Rights Reserved
 */
public interface AccountingSourceFactory {
    /**
     * Returns whether there can only be one AccountingSource created and active at a time.
     *
     * @return singleton or not
     */
    public boolean isSingleton();

    /**
     * Returns the group name.
     *
     * @return group name
     */
    public String getGroup();

    /**
     * Returns the name of this factory
     *
     * @return factory name
     */
    public String getName();

    /**
     * Returns a map of Parameter objects required or optional, passed to the create method.
     *
     * @return map of Parameter objects
     */
    public Map getParameters();

    /**
     * Create a AccountingSource object.
     *
     * @param parameters parameter map of name/value pairs (both String)
     * @return AccountingSource object
     * @throws Exception if anything goes wrong
     */
    public AccountingSource create(Map parameters) throws Exception;
}
