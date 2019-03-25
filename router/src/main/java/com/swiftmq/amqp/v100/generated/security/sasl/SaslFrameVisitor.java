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

package com.swiftmq.amqp.v100.generated.security.sasl;

import com.swiftmq.amqp.v100.transport.HeartbeatFrame;

/**
 * The SaslFrame visitor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public interface SaslFrameVisitor {

    /**
     * Visitor method for a SaslMechanismsFrame type object.
     *
     * @param impl a SaslMechanismsFrame type object
     */
    public void visit(SaslMechanismsFrame impl);

    /**
     * Visitor method for a SaslInitFrame type object.
     *
     * @param impl a SaslInitFrame type object
     */
    public void visit(SaslInitFrame impl);

    /**
     * Visitor method for a SaslChallengeFrame type object.
     *
     * @param impl a SaslChallengeFrame type object
     */
    public void visit(SaslChallengeFrame impl);

    /**
     * Visitor method for a SaslResponseFrame type object.
     *
     * @param impl a SaslResponseFrame type object
     */
    public void visit(SaslResponseFrame impl);

    /**
     * Visitor method for a SaslOutcomeFrame type object.
     *
     * @param impl a SaslOutcomeFrame type object
     */
    public void visit(SaslOutcomeFrame impl);

    /**
     * Visitor method for a HeartbeatFrame type object.
     *
     * @param impl a HeartbeatFrame type object
     */
    public void visit(HeartbeatFrame impl);
}
