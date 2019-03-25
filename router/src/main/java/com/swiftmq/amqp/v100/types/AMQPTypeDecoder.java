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

package com.swiftmq.amqp.v100.types;

import java.io.DataInput;
import java.io.IOException;

/**
 * Decoder class for AMQP types.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPTypeDecoder {
    public static final int CONSTRUCTOR = 0x00;
    public static final int NULL = 0x40;
    public static final int TRUE = 0x41;
    public static final int FALSE = 0x42;
    public static final int BOOLEAN = 0x56;
    public static final int UBYTE = 0x50;
    public static final int USHORT = 0x60;
    public static final int UINT = 0x70;
    public static final int SUINT = 0x52;
    public static final int UINT0 = 0x43;
    public static final int ULONG = 0x80;
    public static final int SULONG = 0x53;
    public static final int ULONG0 = 0x44;
    public static final int BYTE = 0x51;
    public static final int SHORT = 0x61;
    public static final int INT = 0x71;
    public static final int SINT = 0x54;
    public static final int LONG = 0x81;
    public static final int SLONG = 0x55;
    public static final int FLOAT = 0x72;
    public static final int DOUBLE = 0x82;
    public static final int DECIMAL32 = 0x74;
    public static final int DECIMAL64 = 0x84;
    public static final int DECIMAL128 = 0x94;
    public static final int CHAR = 0x73;
    public static final int TIMESTAMP = 0x83;
    public static final int UUID = 0x98;
    public static final int BIN8 = 0xa0;
    public static final int BIN32 = 0xb0;
    public static final int STR8UTF8 = 0xa1;
    public static final int STR32UTF8 = 0xb1;
    public static final int SYM8 = 0xa3;
    public static final int SYM32 = 0xb3;
    public static final int LIST0 = 0x45;
    public static final int LIST8 = 0xc0;
    public static final int LIST32 = 0xd0;
    public static final int ARRAY8 = 0xe0;
    public static final int ARRAY32 = 0xf0;
    public static final int MAP8 = 0xc1;
    public static final int MAP32 = 0xd1;
    public static final int UNKNOWN = 0xff;

    private static boolean descriptorTypeValid(int code) {
        return code == SYM8 || code == SYM32 || code == ULONG || code == SULONG || code == TRUE;
    }

    /**
     * Returns whether the type code represents a string
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isString(int code) {
        return code == STR8UTF8 || code == STR32UTF8;
    }

    /**
     * Returns whether the type code represents a boolean
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isBoolean(int code) {
        return code == TRUE || code == FALSE || code == BOOLEAN;
    }

    /**
     * Returns whether the type code represents an int
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isInt(int code) {
        return code == INT || code == SINT;
    }

    /**
     * Returns whether the type code represents a long
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isLong(int code) {
        return code == LONG || code == SLONG;
    }

    /**
     * Returns whether the type code represents a unsigned int
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isUInt(int code) {
        return code == UINT || code == SUINT || code == UINT0;
    }

    /**
     * Returns whether the type code represents an unsigned long
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isULong(int code) {
        return code == ULONG || code == SULONG || code == ULONG0;
    }

    /**
     * Returns whether the type code represents a symbol
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isSymbol(int code) {
        return code == SYM8 || code == SYM32;
    }

    /**
     * Returns whether the type code represents a list
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isList(int code) {
        return code == LIST0 || code == LIST8 || code == LIST32;
    }

    /**
     * Returns whether the type code represents a msp
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isMap(int code) {
        return code == MAP8 || code == MAP32;
    }

    /**
     * Returns whether the type code represents an array
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isArray(int code) {
        return code == ARRAY8 || code == ARRAY32;
    }

    /**
     * Returns whether the type code represents a binary
     *
     * @param code type code
     * @return true/false
     */
    public static boolean isBinary(int code) {
        return code == BIN8 || code == BIN32;
    }

    /**
     * Reads the type code from the data input and subsequently reads the AMQP type object out of it.
     *
     * @param in DataInput
     * @return AMQPType object
     * @throws IOException on error
     */
    public static AMQPType decode(DataInput in) throws IOException {
        int code = in.readUnsignedByte();
        return decode(code, in);
    }

    /**
     * Reads the AMQP type specified by code from the data input.
     *
     * @param code type code
     * @param in   DataInput
     * @return AMQPType
     * @throws IOException on error
     */
    public static AMQPType decode(int code, DataInput in) throws IOException {
        AMQPType obj = null;
        switch (code) {
            case CONSTRUCTOR:
                AMQPDescribedConstructor constructor = new AMQPDescribedConstructor();
                constructor.readContent(in);
                if (!descriptorTypeValid(constructor.getDescriptor().getCode()))
                    throw new IOException("Invalid descriptor type: 0x" + Integer.toHexString(constructor.getDescriptor().getCode()) + ", must be Symbol or ULong or TRUE!");
                obj = decode(constructor.getFormatCode(), in);
                obj.setConstructor(constructor);
                return obj;
            case NULL:
                obj = new AMQPNull();
                break;
            case TRUE:
            case FALSE:
            case BOOLEAN:
                obj = new AMQPBoolean(code);
                break;
            case UBYTE:
                obj = new AMQPUnsignedByte();
                break;
            case USHORT:
                obj = new AMQPUnsignedShort();
                break;
            case UINT:
            case SUINT:
            case UINT0:
                obj = new AMQPUnsignedInt();
                break;
            case ULONG:
            case SULONG:
            case ULONG0:
                obj = new AMQPUnsignedLong();
                break;
            case BYTE:
                obj = new AMQPByte();
                break;
            case SHORT:
                obj = new AMQPShort();
                break;
            case INT:
            case SINT:
                obj = new AMQPInt();
                break;
            case LONG:
            case SLONG:
                obj = new AMQPLong();
                break;
            case FLOAT:
                obj = new AMQPFloat();
                break;
            case DOUBLE:
                obj = new AMQPDouble();
                break;
            case DECIMAL32:
                obj = new AMQPDecimal32();
                break;
            case DECIMAL64:
                obj = new AMQPDecimal64();
                break;
            case DECIMAL128:
                obj = new AMQPDecimal128();
                break;
            case CHAR:
                obj = new AMQPChar();
                break;
            case TIMESTAMP:
                obj = new AMQPTimestamp();
                break;
            case UUID:
                obj = new AMQPUuid();
                break;
            case BIN8:
            case BIN32:
                obj = new AMQPBinary();
                break;
            case STR8UTF8:
            case STR32UTF8:
                obj = new AMQPString();
                break;
            case SYM8:
            case SYM32:
                obj = new AMQPSymbol();
                break;
            case LIST0:
            case LIST8:
            case LIST32:
                obj = new AMQPList();
                break;
            case ARRAY8:
            case ARRAY32:
                obj = new AMQPArray();
                break;
            case MAP8:
            case MAP32:
                obj = new AMQPMap();
                break;
            default:
                throw new IOException("Can't decode, invalide code: " + "0x" + Integer.toHexString(code));
        }
        obj.setCode(code);
        obj.readContent(in);
        return obj;
    }
}
