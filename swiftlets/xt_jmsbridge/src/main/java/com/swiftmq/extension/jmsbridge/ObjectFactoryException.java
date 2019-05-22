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

package com.swiftmq.extension.jmsbridge;

/**
 * Exception to throw if something goes wrong within
 * the <tt>ObjectFactory</tt>'s accessor methods.
 *
 * @author IIT GmbH, Bremen/Germany
 * @version 1.0
 * @see ObjectFactory
 **/

public class ObjectFactoryException extends Exception {
    Exception linkedException;

    /**
     * Create a new ObjectFactoryException
     *
     * @param message         exception message
     * @param linkedException an optional exception that causes this exception
     */
    public ObjectFactoryException(String message, Exception linkedException) {
        super(message);

        // SBgen: Assign variable
        this.linkedException = linkedException;
    }

    /**
     * Returns the linked excepion
     *
     * @return linked exception
     */
    public Exception getLinkedException() {
        // SBgen: Get variable
        return (linkedException);
    }
}

