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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.UnsupportedEncodingException;

public class PlainServer implements SaslServer
{
  public static final String MECHNAME = "PLAIN";

  CallbackHandler callbackHandler = null;
  String userName = null;
  boolean complete = false;

  public PlainServer(CallbackHandler callbackHandler)
  {
    this.callbackHandler = callbackHandler;
  }

  private int nextOccurance(byte[] src, int pos, byte b)
  {
    for (int i = pos; i < src.length; i++)
      if (src[i] == b)
        return i;
    return src.length;
  }

  private String[] parseResponse(byte[] response)
  {
    String[] s = new String[3];
    try
    {
      int idx = 0;
      int x0 = nextOccurance(response, 0, (byte) 0);
      if (x0 > 0)
        s[idx] = new String(response, 1, x0, "UTF-8");
      int x1 = nextOccurance(response, x0 + 1, (byte) 0);
      s[++idx] = new String(response, x0 + 1, x1 - (x0 + 1), "UTF-8");
      int x2 = nextOccurance(response, x1 + 1, (byte) 0);
      s[++idx] = new String(response, x1 + 1, x2 - (x1 + 1), "UTF-8");
    } catch (UnsupportedEncodingException e)
    {
      e.printStackTrace();
    }
    return s;
  }

  public String getMechanismName()
  {
    return MECHNAME;
  }

  public byte[] evaluateResponse(byte[] response) throws SaslException
  {
    String[] s = parseResponse(response);
    if (s[1] == null || s[2] == null)
      throw new SaslException("No username or password given");

    userName = s[1];
    NameCallback ncb = new NameCallback("prompt", userName);
    PasswordCallback pcb = new PasswordCallback("prompt", false);
    AuthorizeCallback azc = new AuthorizeCallback(userName, userName);
    try
    {
      callbackHandler.handle(new Callback[]{ncb, pcb, azc});
    } catch (Exception e)
    {
      throw new SaslException(e.toString());
    }
    if (!azc.isAuthorized())
      throw new SaslException("Invalid userName");
    char[] pwd = pcb.getPassword();
    if (!s[2].equals(String.valueOf(pwd)))
      throw new SaslException("Invalid Password");
    complete = true;
    return null;
  }

  public boolean isComplete()
  {
    return complete;
  }

  public String getAuthorizationID()
  {
    return userName;
  }

  public byte[] unwrap(byte[] bytes, int i, int i1) throws SaslException
  {
    throw new SaslException("Invalid operation: unwrap");
  }

  public byte[] wrap(byte[] bytes, int i, int i1) throws SaslException
  {
    throw new SaslException("Invalid operation: wrap");
  }

  public Object getNegotiatedProperty(String s)
  {
    return null;
  }

  public void dispose() throws SaslException
  {
  }
}
