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

package com.swiftmq.amqp.v091.generated.tx;

/**
 * The Tx method visitor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version 091. Generation Date: Thu Apr 12 12:18:24 CEST 2012
 **/

public interface TxMethodVisitor {

    /**
     * Visitor method for a Select type object.
     *
     * @param impl a Select type object
     */
    public void visit(Select impl);

    /**
     * Visitor method for a SelectOk type object.
     *
     * @param impl a SelectOk type object
     */
    public void visit(SelectOk impl);

    /**
     * Visitor method for a Commit type object.
     *
     * @param impl a Commit type object
     */
    public void visit(Commit impl);

    /**
     * Visitor method for a CommitOk type object.
     *
     * @param impl a CommitOk type object
     */
    public void visit(CommitOk impl);

    /**
     * Visitor method for a Rollback type object.
     *
     * @param impl a Rollback type object
     */
    public void visit(Rollback impl);

    /**
     * Visitor method for a RollbackOk type object.
     *
     * @param impl a RollbackOk type object
     */
    public void visit(RollbackOk impl);
}
