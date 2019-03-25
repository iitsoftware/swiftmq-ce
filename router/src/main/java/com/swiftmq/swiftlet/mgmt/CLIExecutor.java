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

package com.swiftmq.swiftlet.mgmt;

/**
 * A CLIExecutor is an entity to execute CLI commands.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface CLIExecutor {

    /**
     * Sets an admin role to use in this executor
     *
     * @param name
     * @throws Exception
     */
    public void setAdminRole(String name) throws Exception;

    /**
     * Returns the current context.
     *
     * @return context
     */
    public String getContext();

    /**
     * Execute a CLI command
     *
     * @param command CLI command
     * @throws Exception on error.
     */
    public void execute(String command) throws Exception;

    /**
     * Execute a CLI command that returns a result
     *
     * @param command CLI command
     * @return result
     * @throws Exception on error.
     */
    public String[] executeWithResult(String command) throws Exception;
}

