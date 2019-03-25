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

package com.swiftmq.impl.amqp.amqp.v01_00_00.transformer;

import com.swiftmq.amqp.v100.types.*;

public class Util {
    static final String PROP_MESSAGE_FORMAT = "MESSAGE_FORMAT";
    static final String PROP_AMQP_NATIVE = "NATIVE";
    static final String PROP_GROUP_ID = "JMSXGroupID";
    static final String PROP_GROUP_SEQ = "JMSXGroupSeq";
    static final String PROP_EXPIRATION_CURRENT_TIME_ADD = "JMS_SWIFTMQ_AMQP_EXP_CURRENT_TIME_ADD";
    static final String PROP_AMQP_TO_ADDRESS = "JMS_SWIFTMQ_AMQP_TO_ADDRESS";

    static Object convertAMQPtoJMS(AMQPType value) throws Exception {
        AMQPDescribedConstructor constructor = value.getConstructor();
        int code = constructor != null ? constructor.getFormatCode() : value.getCode();
        Object r = null;
        switch (code) {
            case AMQPTypeDecoder.BOOLEAN:
            case AMQPTypeDecoder.TRUE:
            case AMQPTypeDecoder.FALSE:
                r = ((AMQPBoolean) value).getValue();
                break;
            case AMQPTypeDecoder.BYTE:
                r = ((AMQPByte) value).getValue();
                break;
            case AMQPTypeDecoder.UBYTE:
                r = ((AMQPUnsignedByte) value).getValue();
                break;
            case AMQPTypeDecoder.SHORT:
                r = (short) ((AMQPShort) value).getValue();
                break;
            case AMQPTypeDecoder.USHORT:
                r = ((AMQPUnsignedShort) value).getValue();
                break;
            case AMQPTypeDecoder.INT:
            case AMQPTypeDecoder.SINT:
                r = ((AMQPInt) value).getValue();
                break;
            case AMQPTypeDecoder.UINT:
            case AMQPTypeDecoder.SUINT:
            case AMQPTypeDecoder.UINT0:
                r = ((AMQPUnsignedInt) value).getValue();
                break;
            case AMQPTypeDecoder.LONG:
            case AMQPTypeDecoder.SLONG:
                r = ((AMQPLong) value).getValue();
                break;
            case AMQPTypeDecoder.ULONG:
            case AMQPTypeDecoder.SULONG:
            case AMQPTypeDecoder.ULONG0:
                r = ((AMQPUnsignedLong) value).getValue();
                break;
            case AMQPTypeDecoder.DOUBLE:
                r = ((AMQPDouble) value).getValue();
                break;
            case AMQPTypeDecoder.FLOAT:
                r = ((AMQPFloat) value).getValue();
                break;
            case AMQPTypeDecoder.CHAR:
                r = Character.valueOf((char) ((AMQPChar) value).getValue());
                break;
            case AMQPTypeDecoder.STR8UTF8:
            case AMQPTypeDecoder.STR32UTF8:
                r = ((AMQPString) value).getValue();
                break;
            case AMQPTypeDecoder.SYM8:
            case AMQPTypeDecoder.SYM32:
                r = ((AMQPSymbol) value).getValue();
                break;
            case AMQPTypeDecoder.TIMESTAMP:
                r = ((AMQPTimestamp) value).getValue();
                break;
            case AMQPTypeDecoder.BIN8:
            case AMQPTypeDecoder.BIN32:
                r = ((AMQPBinary) value).getValue();
                break;
            case AMQPTypeDecoder.NULL:
                r = "";
                break;
            default:
                throw new Exception("Unable to transform " + value + " to a JMS type");
        }
        return r;
    }

    static AMQPType convertJMStoAMQP(Object jmsType) {
        if (jmsType == null)
            return new AMQPNull();
        if (jmsType instanceof Byte)
            return new AMQPByte(((Byte) jmsType).byteValue());
        if (jmsType instanceof Character)
            return new AMQPChar(((Character) jmsType).charValue());
        if (jmsType instanceof Boolean)
            return new AMQPBoolean(((Boolean) jmsType).booleanValue());
        if (jmsType instanceof Short)
            return new AMQPShort(((Short) jmsType).shortValue());
        if (jmsType instanceof Integer) {
            int val = ((Integer) jmsType).intValue();
//      if (val >= 0 && val <= Math.pow(2, 8) - 1)
//        return new AMQPUnsignedByte(val);
//      if (val >= 0 && val <= Math.pow(2, 16) - 1)
//        return new AMQPUnsignedShort(val);
            return new AMQPInt(val);
        }
        if (jmsType instanceof Long) {
            long val = ((Long) jmsType).longValue();
//      if (val >= 0 && val <= Math.pow(2, 32) - 1)
//        return new AMQPUnsignedInt(val);
            return new AMQPLong(val);
        }
        if (jmsType instanceof Double)
            return new AMQPDouble(((Double) jmsType).doubleValue());
        if (jmsType instanceof Float)
            return new AMQPFloat(((Float) jmsType).floatValue());
        if (jmsType instanceof String)
            return new AMQPString((String) jmsType);
        if (jmsType instanceof byte[])
            return new AMQPBinary((byte[]) jmsType);
        return null;
    }
}