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

package com.swiftmq.jms.smqp.v750;

/** SMQP-Protocol Version 750, Class: DisconnectReply
 *  Automatically generated, don't change!
 *  Generation Date: Tue Apr 21 10:39:21 CEST 2009
 *  (c) 2009, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v750.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class DisconnectReply extends ReplyNE
{

  protected DisconnectReply()
  {
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_DISCONNECT_REP;
  }


  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v750/DisconnectReply, ");
    _b.append(super.toString());
    _b.append("]");
    return _b.toString();
  }
}
