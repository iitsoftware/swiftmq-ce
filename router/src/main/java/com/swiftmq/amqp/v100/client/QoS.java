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

package com.swiftmq.amqp.v100.client;

/**
 * <p>
 * Quality of Service for message transfer.
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class QoS {
    /**
     * Message transfer is settled before send.
     */
    public static final int AT_MOST_ONCE = 0;

    /**
     * Message transfer is settled after receive.
     */
    public static final int AT_LEAST_ONCE = 1;

    /**
     * Message transfer is settled in a 2 way fashion: First after receive at the receiver and thereafter at the sender.
     */
    public static final int EXACTLY_ONCE = 2;

    /**
     * Verifies a given QoS parameter.
     *
     * @param qos quality of service
     * @throws InvalidQualityOfServiceException if it is invalid
     */
    public static void verify(int qos) throws InvalidQualityOfServiceException {
        if (!(qos == AT_MOST_ONCE ||
                qos == AT_LEAST_ONCE ||
                qos == EXACTLY_ONCE))
            throw new InvalidQualityOfServiceException("Invalid: " + qos);
    }
}
