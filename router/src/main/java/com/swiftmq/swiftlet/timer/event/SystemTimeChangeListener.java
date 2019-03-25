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

package com.swiftmq.swiftlet.timer.event;

import java.util.EventListener;

/**
 * A listener, called when the a system time change has been detected
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2009, All Rights Reserved
 */
public interface SystemTimeChangeListener extends EventListener {
    /**
     * Called when a system time change has been detected
     *
     * @param approxDelta the approximate delta to the previous time in milliseconds
     */
    public void systemTimeChangeDetected(long approxDelta);
}