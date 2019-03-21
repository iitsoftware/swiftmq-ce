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

package com.swiftmq.impl.jms.standard.accounting;

import java.util.regex.Pattern;

public class AccountingProfile
{
  JMSSource source = null;
  Pattern userNameFilter = null;
  Pattern clientIdFilter = null;
  Pattern hostNameFilter = null;
  Pattern queueNameFilter = null;
  Pattern topicNameFilter = null;
  boolean userNameFilterNegate = false;
  boolean clientIdFilterNegate = false;
  boolean hostNameFilterNegate = false;
  boolean queueNameFilterNegate = false;
  boolean topicNameFilterNegate = false;

  public AccountingProfile(Pattern userNameFilter, boolean userNameFilterNegate,
                           Pattern clientIdFilter, boolean clientIdFilterNegate,
                           Pattern hostNameFilter, boolean hostNameFilterNegate,
                           Pattern queueNameFilter, boolean queueNameFilterNegate,
                           Pattern topicNameFilter, boolean topicNameFilterNegate)
  {
    this.userNameFilter = userNameFilter;
    this.userNameFilterNegate = userNameFilterNegate;
    this.clientIdFilter = clientIdFilter;
    this.clientIdFilterNegate = clientIdFilterNegate;
    this.hostNameFilter = hostNameFilter;
    this.hostNameFilterNegate = hostNameFilterNegate;
    this.queueNameFilter = queueNameFilter;
    this.queueNameFilterNegate = queueNameFilterNegate;
    this.topicNameFilter = topicNameFilter;
    this.topicNameFilterNegate = topicNameFilterNegate;
  }

  public void setSource(JMSSource source)
  {
    this.source = source;
  }

  public JMSSource getSource()
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

  public boolean isMatchUserName(String userName)
  {
    return checkMatch(userNameFilter, userNameFilterNegate, userName);
  }

  public boolean isMatchClientId(String clientId)
  {
    return checkMatch(clientIdFilter, clientIdFilterNegate, clientId);
  }

  public boolean isMatchHostName(String hostName)
  {
    return checkMatch(hostNameFilter, hostNameFilterNegate, hostName);
  }

  public boolean isMatchQueueName(String queueName)
  {
    return checkMatch(queueNameFilter, queueNameFilterNegate, queueName);
  }

  public boolean isMatchTopicName(String topicName)
  {
    return checkMatch(topicNameFilter, topicNameFilterNegate, topicName);
  }

  public String toString()
  {
    return "[AccountingProfile, source=" + source + ", userNameFilter=" + userNameFilter + ", userNameFilterNegate=" + userNameFilterNegate + ", clientIdFilter=" + clientIdFilter + ", clientIdFilterNegate=" + clientIdFilterNegate +
        ", hostNameFilter=" + hostNameFilter + ", hostNameFilterNegate=" + hostNameFilterNegate + ", queueNameFilter=" + queueNameFilter + ", queueNameFilterNegate=" + queueNameFilterNegate +
        ", topicNameFilter=" + topicNameFilter + ", topicNameFilterNegate=" + topicNameFilterNegate + "]";
  }
}
