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

package com.swiftmq.amqp.v091.generated.queue;

/**
 * The Queue method visitor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version 091. Generation Date: Thu Apr 12 12:18:24 CEST 2012
 **/

public interface QueueMethodVisitor {

    /**
     * Visitor method for a Declare type object.
     *
     * @param impl a Declare type object
     */
    public void visit(Declare impl);

    /**
     * Visitor method for a DeclareOk type object.
     *
     * @param impl a DeclareOk type object
     */
    public void visit(DeclareOk impl);

    /**
     * Visitor method for a Bind type object.
     *
     * @param impl a Bind type object
     */
    public void visit(Bind impl);

    /**
     * Visitor method for a BindOk type object.
     *
     * @param impl a BindOk type object
     */
    public void visit(BindOk impl);

    /**
     * Visitor method for a Unbind type object.
     *
     * @param impl a Unbind type object
     */
    public void visit(Unbind impl);

    /**
     * Visitor method for a UnbindOk type object.
     *
     * @param impl a UnbindOk type object
     */
    public void visit(UnbindOk impl);

    /**
     * Visitor method for a Purge type object.
     *
     * @param impl a Purge type object
     */
    public void visit(Purge impl);

    /**
     * Visitor method for a PurgeOk type object.
     *
     * @param impl a PurgeOk type object
     */
    public void visit(PurgeOk impl);

    /**
     * Visitor method for a Delete type object.
     *
     * @param impl a Delete type object
     */
    public void visit(Delete impl);

    /**
     * Visitor method for a DeleteOk type object.
     *
     * @param impl a DeleteOk type object
     */
    public void visit(DeleteOk impl);
}
