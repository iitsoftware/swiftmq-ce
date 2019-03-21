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
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

public class AnonServer implements SaslServer
{
  public static final String MECHNAME = "ANONYMOUS";

  CallbackHandler callbackHandler = null;
  boolean done = false;

  public AnonServer(CallbackHandler callbackHandler)
  {
    this.callbackHandler = callbackHandler;
  }

  public String getMechanismName()
  {
    return MECHNAME;
  }

  public byte[] evaluateResponse(byte[] bytes) throws SaslException
  {
    done = true;
    return null;
  }

  public boolean isComplete()
  {
    return done;
  }

  public String getAuthorizationID()
  {
    return "anonymous";
  }

  public byte[] unwrap(byte[] bytes, int i, int i1) throws SaslException
  {
    throw new SaslException("Operation not supported");
  }

  public byte[] wrap(byte[] bytes, int i, int i1) throws SaslException
  {
    throw new SaslException("Operation not supported");
  }

  public Object getNegotiatedProperty(String s)
  {
    return null;
  }

  public void dispose() throws SaslException
  {
  }
}
