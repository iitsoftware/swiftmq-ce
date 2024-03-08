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

package jms.base;

import com.swiftmq.jms.TopicImpl;

import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.InitialContext;

public class PSTestCase extends JMSTestCase {
    public InitialContext ctx = null;
    public TopicConnectionFactory tcf = null;

    public PSTestCase(String name) {
        super(name);
    }

    public TopicConnection createTopicConnection(InitialContext ctx, String lookup) {
        return createTopicConnection(ctx, lookup, null);
    }

    public TopicConnection createTopicConnection(InitialContext ctx, String lookup, String clientId, boolean start) {
        TopicConnection tc = null;
        try {
            if (tcf == null)
                tcf = (TopicConnectionFactory) ctx.lookup(lookup);
            tc = tcf.createTopicConnection();
            if (clientId != null)
                tc.setClientID(clientId);
            if (start)
                tc.start();
        } catch (Exception e) {
            failFast("create topic connection failed: " + e);
        }
        return tc;
    }

    public TopicConnection createTopicConnection(InitialContext ctx, String lookup, String clientId) {
        return createTopicConnection(ctx, lookup, clientId, true);
    }

    public TopicConnection createTopicConnection(String lookup, String clientId, boolean start) {
        TopicConnection tc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            tc = createTopicConnection(ctx, lookup, clientId, start);
        } catch (Exception e) {
            failFast("create topic connection failed: " + e);
        }
        return tc;
    }

    public TopicConnection createTopicConnection(String lookup, String clientId) {
        TopicConnection tc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            tc = createTopicConnection(ctx, lookup, clientId, true);
        } catch (Exception e) {
            failFast("create topic connection failed: " + e);
        }
        return tc;
    }

    public TopicConnection createTopicConnection(String lookup, boolean start) {
        return createTopicConnection(lookup, null, start);
    }

    public TopicConnection createTopicConnection(String lookup) {
        return createTopicConnection(lookup, null, true);
    }

    public Topic getTopic(String name) {
        return new TopicImpl(name);
    }

    protected void tearDown() throws Exception {
        if (ctx != null)
            ctx.close();
        ctx = null;
        tcf = null;
        super.tearDown();
    }
}

