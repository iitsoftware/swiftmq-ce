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

package com.swiftmq.amqp.v100.generated.transport.performatives;

import com.swiftmq.amqp.v100.types.*;

import java.io.IOException;

/**
 * Factory class to create FrameIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class FrameFactory {

    /**
     * Creates a FrameIF object.
     *
     * @param channel the channel
     * @param bare    the bare AMQP type
     * @return FrameIF
     */
    public static FrameIF create(int channel, AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        AMQPDescribedConstructor constructor = bare.getConstructor();
        if (constructor == null)
            throw new IOException("Missing constructor: " + bare);
        AMQPType descriptor = constructor.getDescriptor();
        int code = descriptor.getCode();
        if (AMQPTypeDecoder.isULong(code)) {
            long type = ((AMQPUnsignedLong) descriptor).getValue();
            if (type == OpenFrame.DESCRIPTOR_CODE)
                return new OpenFrame(channel, (AMQPList) bare);
            if (type == BeginFrame.DESCRIPTOR_CODE)
                return new BeginFrame(channel, (AMQPList) bare);
            if (type == AttachFrame.DESCRIPTOR_CODE)
                return new AttachFrame(channel, (AMQPList) bare);
            if (type == FlowFrame.DESCRIPTOR_CODE)
                return new FlowFrame(channel, (AMQPList) bare);
            if (type == TransferFrame.DESCRIPTOR_CODE)
                return new TransferFrame(channel, (AMQPList) bare);
            if (type == DispositionFrame.DESCRIPTOR_CODE)
                return new DispositionFrame(channel, (AMQPList) bare);
            if (type == DetachFrame.DESCRIPTOR_CODE)
                return new DetachFrame(channel, (AMQPList) bare);
            if (type == EndFrame.DESCRIPTOR_CODE)
                return new EndFrame(channel, (AMQPList) bare);
            if (type == CloseFrame.DESCRIPTOR_CODE)
                return new CloseFrame(channel, (AMQPList) bare);
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else if (AMQPTypeDecoder.isSymbol(code)) {
            String type = ((AMQPSymbol) descriptor).getValue();
            if (type.equals(OpenFrame.DESCRIPTOR_NAME))
                return new OpenFrame(channel, (AMQPList) bare);
            if (type.equals(BeginFrame.DESCRIPTOR_NAME))
                return new BeginFrame(channel, (AMQPList) bare);
            if (type.equals(AttachFrame.DESCRIPTOR_NAME))
                return new AttachFrame(channel, (AMQPList) bare);
            if (type.equals(FlowFrame.DESCRIPTOR_NAME))
                return new FlowFrame(channel, (AMQPList) bare);
            if (type.equals(TransferFrame.DESCRIPTOR_NAME))
                return new TransferFrame(channel, (AMQPList) bare);
            if (type.equals(DispositionFrame.DESCRIPTOR_NAME))
                return new DispositionFrame(channel, (AMQPList) bare);
            if (type.equals(DetachFrame.DESCRIPTOR_NAME))
                return new DetachFrame(channel, (AMQPList) bare);
            if (type.equals(EndFrame.DESCRIPTOR_NAME))
                return new EndFrame(channel, (AMQPList) bare);
            if (type.equals(CloseFrame.DESCRIPTOR_NAME))
                return new CloseFrame(channel, (AMQPList) bare);
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else
            throw new Exception("Invalid type of constructor descriptor (actual type=" + code + ", expected=symbold or ulong), bare= " + bare);
    }

}
