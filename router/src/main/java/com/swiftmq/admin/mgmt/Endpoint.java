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

package com.swiftmq.admin.mgmt;

import com.swiftmq.mgmt.CommandExecutor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRegistry;

public interface Endpoint extends CommandExecutor {
    void connect(int connectId, String hostname, String toolName, boolean subscribeRouteInfos, boolean subscribeRouterConfig, boolean subscribeChangeEvents) throws Exception;

    boolean isAuthenticationRequired();

    void authenticate(String password) throws Exception;

    boolean isStarted();

    void setStarted(boolean started);

    boolean isRouteInfos();

    void setRouteInfos(boolean routeInfos);

    RequestRegistry getRequestRegistry();

    void setRouterName(String routerName);

    String getRouterName();

    String[] getActContext();

    void setActContext(String[] actContext);

    boolean isSubscriptionFilterEnabled();

    void setSubscriptionFilterEnabled(boolean subscriptionFilterEnabled);

    void contextShown(String[] context, boolean includeNextLevel);

    void contextHidden(String[] context, boolean includeNextLevel);

    void startLease(long interval);

    Reply request(Request request) throws Exception;

    String[] execute(String[] context, Entity entity, String[] command);

    void close();
}
