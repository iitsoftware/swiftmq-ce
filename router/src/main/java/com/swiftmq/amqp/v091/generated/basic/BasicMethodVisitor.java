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

package com.swiftmq.amqp.v091.generated.basic;

/**
 * The Basic method visitor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version 091. Generation Date: Thu Apr 12 12:18:24 CEST 2012
 **/

public interface BasicMethodVisitor {

    /**
     * Visitor method for a Qos type object.
     *
     * @param impl a Qos type object
     */
    public void visit(Qos impl);

    /**
     * Visitor method for a QosOk type object.
     *
     * @param impl a QosOk type object
     */
    public void visit(QosOk impl);

    /**
     * Visitor method for a Consume type object.
     *
     * @param impl a Consume type object
     */
    public void visit(Consume impl);

    /**
     * Visitor method for a ConsumeOk type object.
     *
     * @param impl a ConsumeOk type object
     */
    public void visit(ConsumeOk impl);

    /**
     * Visitor method for a Cancel type object.
     *
     * @param impl a Cancel type object
     */
    public void visit(Cancel impl);

    /**
     * Visitor method for a CancelOk type object.
     *
     * @param impl a CancelOk type object
     */
    public void visit(CancelOk impl);

    /**
     * Visitor method for a Publish type object.
     *
     * @param impl a Publish type object
     */
    public void visit(Publish impl);

    /**
     * Visitor method for a Return type object.
     *
     * @param impl a Return type object
     */
    public void visit(Return impl);

    /**
     * Visitor method for a Deliver type object.
     *
     * @param impl a Deliver type object
     */
    public void visit(Deliver impl);

    /**
     * Visitor method for a Get type object.
     *
     * @param impl a Get type object
     */
    public void visit(Get impl);

    /**
     * Visitor method for a GetOk type object.
     *
     * @param impl a GetOk type object
     */
    public void visit(GetOk impl);

    /**
     * Visitor method for a GetEmpty type object.
     *
     * @param impl a GetEmpty type object
     */
    public void visit(GetEmpty impl);

    /**
     * Visitor method for a Ack type object.
     *
     * @param impl a Ack type object
     */
    public void visit(Ack impl);

    /**
     * Visitor method for a Reject type object.
     *
     * @param impl a Reject type object
     */
    public void visit(Reject impl);

    /**
     * Visitor method for a RecoverAsync type object.
     *
     * @param impl a RecoverAsync type object
     */
    public void visit(RecoverAsync impl);

    /**
     * Visitor method for a Recover type object.
     *
     * @param impl a Recover type object
     */
    public void visit(Recover impl);

    /**
     * Visitor method for a RecoverOk type object.
     *
     * @param impl a RecoverOk type object
     */
    public void visit(RecoverOk impl);

    /**
     * Visitor method for a Nack type object.
     *
     * @param impl a Nack type object
     */
    public void visit(Nack impl);
}
