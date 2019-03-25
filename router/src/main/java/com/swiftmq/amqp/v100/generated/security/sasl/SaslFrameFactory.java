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

package com.swiftmq.amqp.v100.generated.security.sasl;

import com.swiftmq.amqp.v100.types.*;

import java.io.IOException;

/**
 * Factory class to create SaslFrameIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class SaslFrameFactory {

    /**
     * Creates a SaslFrameIF object.
     *
     * @param channel the channel
     * @param bare    the bare AMQP type
     * @return SaslFrameIF
     */
    public static SaslFrameIF create(int channel, AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        AMQPDescribedConstructor constructor = bare.getConstructor();
        if (constructor == null)
            throw new IOException("Missing constructor: " + bare);
        AMQPType descriptor = constructor.getDescriptor();
        int code = descriptor.getCode();
        if (AMQPTypeDecoder.isULong(code)) {
            long type = ((AMQPUnsignedLong) descriptor).getValue();
            if (type == SaslMechanismsFrame.DESCRIPTOR_CODE)
                return new SaslMechanismsFrame(channel, (AMQPList) bare);
            if (type == SaslInitFrame.DESCRIPTOR_CODE)
                return new SaslInitFrame(channel, (AMQPList) bare);
            if (type == SaslChallengeFrame.DESCRIPTOR_CODE)
                return new SaslChallengeFrame(channel, (AMQPList) bare);
            if (type == SaslResponseFrame.DESCRIPTOR_CODE)
                return new SaslResponseFrame(channel, (AMQPList) bare);
            if (type == SaslOutcomeFrame.DESCRIPTOR_CODE)
                return new SaslOutcomeFrame(channel, (AMQPList) bare);
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else if (AMQPTypeDecoder.isSymbol(code)) {
            String type = ((AMQPSymbol) descriptor).getValue();
            if (type.equals(SaslMechanismsFrame.DESCRIPTOR_NAME))
                return new SaslMechanismsFrame(channel, (AMQPList) bare);
            if (type.equals(SaslInitFrame.DESCRIPTOR_NAME))
                return new SaslInitFrame(channel, (AMQPList) bare);
            if (type.equals(SaslChallengeFrame.DESCRIPTOR_NAME))
                return new SaslChallengeFrame(channel, (AMQPList) bare);
            if (type.equals(SaslResponseFrame.DESCRIPTOR_NAME))
                return new SaslResponseFrame(channel, (AMQPList) bare);
            if (type.equals(SaslOutcomeFrame.DESCRIPTOR_NAME))
                return new SaslOutcomeFrame(channel, (AMQPList) bare);
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else
            throw new Exception("Invalid type of constructor descriptor (actual type=" + code + ", expected=symbold or ulong), bare= " + bare);
    }

}
