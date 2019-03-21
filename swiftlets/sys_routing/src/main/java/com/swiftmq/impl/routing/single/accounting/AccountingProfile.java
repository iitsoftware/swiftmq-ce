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

package com.swiftmq.impl.routing.single.accounting;

import java.util.regex.Pattern;

public class AccountingProfile
{
  RoutingSource source = null;
  Pattern sourceRouterFilter = null;
  boolean sourceRouterFilterNegate = false;
  Pattern destinationRouterFilter = null;
  boolean destinationRouterFilterNegate = false;
  Pattern connectedRouterFilter = null;
  boolean connectedRouterFilterNegate = false;
  Pattern queueNameFilter = null;
  boolean queueNameFilterNegate = false;
  Pattern topicNameFilter = null;
  boolean topicNameFilterNegate = false;
  boolean inbound = false;
  boolean outbound = false;

  public AccountingProfile(Pattern sourceRouterFilter, boolean sourceRouterFilterNegate,
                           Pattern destinationRouterFilter, boolean destinationRouterFilterNegate,
                           Pattern connectedRouterFilter, boolean connectedRouterFilterNegate,
                           Pattern queueNameFilter, boolean queueNameFilterNegate,
                           Pattern topicNameFilter, boolean topicNameFilterNegate,
                           boolean inbound, boolean outbound)
  {
    this.sourceRouterFilter = sourceRouterFilter;
    this.sourceRouterFilterNegate = sourceRouterFilterNegate;
    this.destinationRouterFilter = destinationRouterFilter;
    this.destinationRouterFilterNegate = destinationRouterFilterNegate;
    this.connectedRouterFilter = connectedRouterFilter;
    this.connectedRouterFilterNegate = connectedRouterFilterNegate;
    this.queueNameFilter = queueNameFilter;
    this.queueNameFilterNegate = queueNameFilterNegate;
    this.topicNameFilter = topicNameFilter;
    this.topicNameFilterNegate = topicNameFilterNegate;
    this.inbound = inbound;
    this.outbound = outbound;
  }

  public void setSource(RoutingSource source)
  {
    this.source = source;
  }

  public RoutingSource getSource()
  {
    return source;
  }

  private boolean checkMatch(Pattern p, boolean negate, String value)
  {
    if (value == null && p != null)
      return false;
    if (p == null)
      return true;
    boolean b = p.matcher(value).matches();
    return negate ? !b : b;
  }

  public boolean isMatchSourceRouter(String routerName)
  {
    return checkMatch(sourceRouterFilter, sourceRouterFilterNegate, routerName);
  }

  public boolean isMatchDestinationRouter(String routerName)
  {
    return checkMatch(destinationRouterFilter, destinationRouterFilterNegate, routerName);
  }

  public boolean isMatchConnectedRouter(String routerName)
  {
    return checkMatch(connectedRouterFilter, connectedRouterFilterNegate, routerName);
  }

  public boolean isMatchQueueName(String queueName)
  {
    return checkMatch(queueNameFilter, queueNameFilterNegate, queueName);
  }

  public boolean isMatchTopicName(String topicName)
  {
    return checkMatch(topicNameFilter, topicNameFilterNegate, topicName);
  }

  public boolean isInbound()
  {
    return inbound;
  }

  public boolean isOutbound()
  {
    return outbound;
  }

  public String toString()
  {
    return "[AccountingProfile, source=" + source +
        ", sourceRouterFilter=" + sourceRouterFilter + ", sourceRouterFilterNegate=" + sourceRouterFilterNegate +
        ", destinationRouterFilter=" + destinationRouterFilter + ", destinationRouterFilterNegate=" + destinationRouterFilterNegate +
        ", connectedRouterFilter=" + connectedRouterFilter + ", connectedRouterFilterNegate=" + connectedRouterFilterNegate +
        ", queueNameFilter=" + queueNameFilter + ", queueNameFilterNegate=" + queueNameFilterNegate +
        ", topicNameFilter=" + topicNameFilter + ", topicNameFilterNegate=" + topicNameFilterNegate +
        ", inbound=" + inbound + ", outbound=" + outbound + "]";
  }
}
