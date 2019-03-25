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

package com.swiftmq.jms.v630;

import com.swiftmq.net.client.Connection;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;

public interface RecreatableConnection extends Recreatable {
    public void prepareForReconnect();

    public Request getVersionRequest();

    public void setVersionReply(Reply reply) throws Exception;

    public Request getAuthenticateRequest();

    public void setAuthenticateReply(Reply reply) throws Exception;

    public Request getAuthenticateResponse();

    public void setAuthenticateResponseReply(Reply reply) throws Exception;

    public Request getMetaDataRequest();

    public void setMetaDataReply(Reply reply) throws Exception;

    public Request getGetClientIdRequest();

    public void setGetClientIdReply(Reply reply) throws Exception;

    public Request getSetClientIdRequest();

    public void setSetClientIdReply(Reply reply) throws Exception;

    public void handOver(Connection connection);

    public void cancelAndNotify(Exception exception, boolean closeReconnector);
}
