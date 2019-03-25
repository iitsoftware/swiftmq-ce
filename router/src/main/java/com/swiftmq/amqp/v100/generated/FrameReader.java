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

package com.swiftmq.amqp.v100.generated;

import com.swiftmq.amqp.v100.generated.security.sasl.SaslFrameFactory;
import com.swiftmq.amqp.v100.generated.security.sasl.SaslFrameIF;
import com.swiftmq.amqp.v100.generated.transport.performatives.FrameFactory;
import com.swiftmq.amqp.v100.generated.transport.performatives.FrameIF;
import com.swiftmq.amqp.v100.transport.AMQPFrame;
import com.swiftmq.amqp.v100.transport.HeartbeatFrame;
import com.swiftmq.amqp.v100.types.AMQPTypeDecoder;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import java.io.IOException;

/**
 * Factory class that reads SASL and AMQP frames out of an input stream.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class FrameReader {

    /**
     * Creates a FrameIF object.
     *
     * @param in input stream
     * @return frame
     */
    public static FrameIF createFrame(LengthCaptureDataInput in) throws Exception {
        long frameSize = 0;
        byte dataOffset = 0;
        byte typeCode = 0;
        int channel = 0;
        byte[] extendedHeader = null;

        in.startCaptureLength();

        // frame header
        frameSize = in.readInt();
        dataOffset = in.readByte();
        typeCode = in.readByte();
        if (!((typeCode == AMQPFrame.TYPE_CODE_AMQP_FRAME) || (typeCode == AMQPFrame.TYPE_CODE_SASL_FRAME)))
            throw new IOException("Invalid frame type (" + typeCode + "), not an AMQP or SASL frame!");
        channel = in.readUnsignedShort();

        // extended header
        int doff = dataOffset;
        if (doff < 2)
            throw new Exception("Malformed frame, data offset is " + doff);
        if (doff > 2) {
            extendedHeader = new byte[doff * 4 - 8];
            in.readFully(extendedHeader);
        }

        // body
        long bodySize = frameSize - doff * 4;
        if (bodySize > 0) {
            if (bodySize > Integer.MAX_VALUE)
                throw new Exception("Frame body size (" + bodySize + ") is greater than Integer.MAX_VALUE (" + Integer.MAX_VALUE + ")");
        } else
            return new HeartbeatFrame(channel);

        AMQPFrame frame = (AMQPFrame) FrameFactory.create(channel, AMQPTypeDecoder.decode(in));
        int plLength = (int) (frameSize - in.stopCaptureLength());
        if (plLength > 0) {
            byte b[] = new byte[plLength];
            in.readFully(b);
            frame.setPayload(b);
        }
        return frame;
    }

    /**
     * Creates a SaslFrameIF object.
     *
     * @param in input stream
     * @return frame
     */
    public static SaslFrameIF createSaslFrame(LengthCaptureDataInput in) throws Exception {
        long frameSize = 0;
        byte dataOffset = 0;
        byte typeCode = 0;
        int channel = 0;
        byte[] extendedHeader = null;

        in.startCaptureLength();

        // frame header
        frameSize = in.readInt();
        dataOffset = in.readByte();
        typeCode = in.readByte();
        if (!((typeCode == AMQPFrame.TYPE_CODE_AMQP_FRAME) || (typeCode == AMQPFrame.TYPE_CODE_SASL_FRAME)))
            throw new IOException("Invalid frame type (" + typeCode + "), not an AMQP or SASL frame!");
        channel = in.readUnsignedShort();

        // extended header
        int doff = dataOffset;
        if (doff < 2)
            throw new Exception("Malformed frame, data offset is " + doff);
        if (doff > 2) {
            extendedHeader = new byte[doff * 4 - 8];
            in.readFully(extendedHeader);
        }

        // body
        long bodySize = frameSize - doff * 4;
        if (bodySize > 0) {
            if (bodySize > Integer.MAX_VALUE)
                throw new Exception("Frame body size (" + bodySize + ") is greater than Integer.MAX_VALUE (" + Integer.MAX_VALUE + ")");
        } else
            return new HeartbeatFrame(channel);

        AMQPFrame frame = (AMQPFrame) SaslFrameFactory.create(channel, AMQPTypeDecoder.decode(in));
        int plLength = (int) (frameSize - in.stopCaptureLength());
        if (plLength > 0) {
            byte b[] = new byte[plLength];
            in.readFully(b);
            frame.setPayload(b);
        }
        return frame;
    }
}
