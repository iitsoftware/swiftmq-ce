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

package com.swiftmq.jms;

import com.swiftmq.tools.requestreply.TimeoutException;
import com.swiftmq.tools.requestreply.TransportException;

import javax.jms.JMSException;

public class ExceptionConverter {
    public static JMSException convert(Exception exception) {
        if (exception instanceof JMSException)
            return (JMSException) exception;
        else if (exception instanceof TimeoutException)
            return new RequestTimeoutException(exception.getMessage(), exception);
        else if (exception instanceof TransportException)
            return new ConnectionLostException(exception.getMessage(), exception);
        else {
            JMSException e = new JMSException(exception.getMessage());
            e.setLinkedException(exception);
            return e;
        }
    }
}



