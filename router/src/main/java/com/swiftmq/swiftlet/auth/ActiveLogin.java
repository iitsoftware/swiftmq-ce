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

package com.swiftmq.swiftlet.auth;

/**
 * Represents an authenticated user
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class ActiveLogin
{
  Object loginId;
  String userName;
  long loginTime;
  String clientId = null;
  String type = null;
  ResourceLimitGroup resourceLimitGroup;

  /**
   * Constructs a new ActiveLogin
   * @param loginId login id
   * @param userName user name
   * @param loginTime login time in ms
   * @param type login type
   * @param resourceLimitGroup resource limit group
   * @SBGen Constructor assigns loginId, userName, loginTime, resourceLimitGroup
   */
  ActiveLogin(Object loginId, String userName, long loginTime, String type, ResourceLimitGroup resourceLimitGroup)
  {
    // SBgen: Assign variables
    this.loginId = loginId;
    this.userName = userName;
    this.loginTime = loginTime;
    this.type = type;
    this.resourceLimitGroup = resourceLimitGroup;
    // SBgen: End assign
  }

  /**
   * Returns the login id
   * @return login id
   * @SBGen Method get loginId
   */
  public Object getLoginId()
  {
    // SBgen: Get variable
    return (loginId);
  }

  /**
   * Sets the client id
   * @param clientId client id
   * @SBGen Method set clientId
   */
  public void setClientId(String clientId)
  {
    // SBgen: Set variable
    this.clientId = clientId;
  }

  /**
   * Returns the client id
   * @return client id
   * @SBGen Method get clientId
   */
  public String getClientId()
  {
    return clientId != null?clientId:loginId.toString();
  }

  /**
   * Returns the user name
   * @return user name
   * @SBGen Method get userName
   */
  public String getUserName()
  {
    // SBgen: Get variable
    return (userName);
  }

  /**
   * Returns the login type
   * @return login type
   */
  public String getType()
  {
    // SBgen: Get variable
    return (type);
  }

  /**
   * Returns the login time
   * @return login time
   * @SBGen Method get loginTime
   */
  public long getLoginTime()
  {
    // SBgen: Get variable
    return (loginTime);
  }

  /**
   * Returns the resource limit group
   * @return resource limit group
   * @SBGen Method get resourceLimitGroup
   */
  public ResourceLimitGroup getResourceLimitGroup()
  {
    // SBgen: Get variable
    return (resourceLimitGroup);
  }

  public String toString()
  {
    return loginId.toString();
  }
}

