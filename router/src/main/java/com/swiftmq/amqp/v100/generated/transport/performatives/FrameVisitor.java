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

package com.swiftmq.amqp.v100.generated.transport.performatives;

import com.swiftmq.amqp.v100.transport.HeartbeatFrame;

/**
 * The Frame visitor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public interface FrameVisitor {

    /**
     * Visitor method for a OpenFrame type object.
     *
     * @param impl a OpenFrame type object
     */
    public void visit(OpenFrame impl);

    /**
     * Visitor method for a BeginFrame type object.
     *
     * @param impl a BeginFrame type object
     */
    public void visit(BeginFrame impl);

    /**
     * Visitor method for a AttachFrame type object.
     *
     * @param impl a AttachFrame type object
     */
    public void visit(AttachFrame impl);

    /**
     * Visitor method for a FlowFrame type object.
     *
     * @param impl a FlowFrame type object
     */
    public void visit(FlowFrame impl);

    /**
     * Visitor method for a TransferFrame type object.
     *
     * @param impl a TransferFrame type object
     */
    public void visit(TransferFrame impl);

    /**
     * Visitor method for a DispositionFrame type object.
     *
     * @param impl a DispositionFrame type object
     */
    public void visit(DispositionFrame impl);

    /**
     * Visitor method for a DetachFrame type object.
     *
     * @param impl a DetachFrame type object
     */
    public void visit(DetachFrame impl);

    /**
     * Visitor method for a EndFrame type object.
     *
     * @param impl a EndFrame type object
     */
    public void visit(EndFrame impl);

    /**
     * Visitor method for a CloseFrame type object.
     *
     * @param impl a CloseFrame type object
     */
    public void visit(CloseFrame impl);

    /**
     * Visitor method for a HeartbeatFrame type object.
     *
     * @param impl a HeartbeatFrame type object
     */
    public void visit(HeartbeatFrame impl);
}
