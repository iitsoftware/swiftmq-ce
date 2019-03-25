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

package com.swiftmq.impl.amqp.sasl.provider;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaslServerFactoryImpl implements SaslServerFactory {
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map props, CallbackHandler callbackHandler) throws SaslException {
        if (mechanism.equals(AnonServer.MECHNAME))
            return new AnonServer(callbackHandler);
        if (mechanism.equals(PlainServer.MECHNAME))
            return new PlainServer(callbackHandler);
        return null;
    }

    public String[] getMechanismNames(Map properties) {
        List list = new ArrayList();
        if (!(properties != null &&
                (properties.containsKey(Sasl.POLICY_NOPLAINTEXT) ||
                        properties.containsKey(Sasl.POLICY_NODICTIONARY) ||
                        properties.containsKey(Sasl.POLICY_NOACTIVE)))) {
            list.add(PlainServer.MECHNAME);
            if (properties == null || !properties.containsKey(Sasl.POLICY_NOANONYMOUS))
                list.add(AnonServer.MECHNAME);
        }
        return (String[]) list.toArray(new String[list.size()]);
    }
}
