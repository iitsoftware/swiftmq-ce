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

package com.swiftmq.swiftlet.xa;

/**
 * This exception class wraps XA error code and message.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public class XAContextException extends Exception {
    int errorCode = 0;
    String message = null;

    public XAContextException(String message) {
        super(message);
        this.message = message;
    }

    public XAContextException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.message = message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public String toString() {
        return "[XAContextException, errorCode=" + errorCode + ", message=" + message + "]";
    }
}
