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

package com.swiftmq.amqp.v100.generated.messaging.message_format;

/**
 * The Section visitor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public interface SectionVisitor {

    /**
     * Visitor method for a Header type object.
     *
     * @param impl a Header type object
     */
    public void visit(Header impl);

    /**
     * Visitor method for a DeliveryAnnotations type object.
     *
     * @param impl a DeliveryAnnotations type object
     */
    public void visit(DeliveryAnnotations impl);

    /**
     * Visitor method for a MessageAnnotations type object.
     *
     * @param impl a MessageAnnotations type object
     */
    public void visit(MessageAnnotations impl);

    /**
     * Visitor method for a Properties type object.
     *
     * @param impl a Properties type object
     */
    public void visit(Properties impl);

    /**
     * Visitor method for a ApplicationProperties type object.
     *
     * @param impl a ApplicationProperties type object
     */
    public void visit(ApplicationProperties impl);

    /**
     * Visitor method for a Data type object.
     *
     * @param impl a Data type object
     */
    public void visit(Data impl);

    /**
     * Visitor method for a AmqpSequence type object.
     *
     * @param impl a AmqpSequence type object
     */
    public void visit(AmqpSequence impl);

    /**
     * Visitor method for a AmqpValue type object.
     *
     * @param impl a AmqpValue type object
     */
    public void visit(AmqpValue impl);

    /**
     * Visitor method for a Footer type object.
     *
     * @param impl a Footer type object
     */
    public void visit(Footer impl);
}
