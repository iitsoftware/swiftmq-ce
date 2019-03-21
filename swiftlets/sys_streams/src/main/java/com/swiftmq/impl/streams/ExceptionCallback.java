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

package com.swiftmq.impl.streams;

/**
 * Callback that can be registered on the Stream (onException).
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public interface ExceptionCallback {

    /**
     * Single method executed as a function call as
     * function(exception, lastStackTrace){...}
     * @param exception The last exception occurred
     * @param lastStackTrace Formatted stack trace of this exception
     */
    public void execute(String exception, String lastStackTrace);
}
