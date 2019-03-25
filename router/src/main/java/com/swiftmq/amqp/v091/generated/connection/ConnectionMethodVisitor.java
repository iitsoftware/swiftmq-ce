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

package com.swiftmq.amqp.v091.generated.connection;

/**
 * The Connection method visitor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version 091. Generation Date: Thu Apr 12 12:18:24 CEST 2012
 **/

public interface ConnectionMethodVisitor {

    /**
     * Visitor method for a Start type object.
     *
     * @param impl a Start type object
     */
    public void visit(Start impl);

    /**
     * Visitor method for a StartOk type object.
     *
     * @param impl a StartOk type object
     */
    public void visit(StartOk impl);

    /**
     * Visitor method for a Secure type object.
     *
     * @param impl a Secure type object
     */
    public void visit(Secure impl);

    /**
     * Visitor method for a SecureOk type object.
     *
     * @param impl a SecureOk type object
     */
    public void visit(SecureOk impl);

    /**
     * Visitor method for a Tune type object.
     *
     * @param impl a Tune type object
     */
    public void visit(Tune impl);

    /**
     * Visitor method for a TuneOk type object.
     *
     * @param impl a TuneOk type object
     */
    public void visit(TuneOk impl);

    /**
     * Visitor method for a Open type object.
     *
     * @param impl a Open type object
     */
    public void visit(Open impl);

    /**
     * Visitor method for a OpenOk type object.
     *
     * @param impl a OpenOk type object
     */
    public void visit(OpenOk impl);

    /**
     * Visitor method for a Close type object.
     *
     * @param impl a Close type object
     */
    public void visit(Close impl);

    /**
     * Visitor method for a CloseOk type object.
     *
     * @param impl a CloseOk type object
     */
    public void visit(CloseOk impl);
}
