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

package com.swiftmq.jms.v610.po;

import com.swiftmq.tools.pipeline.POVisitor;

public interface ReconnectVisitor extends POVisitor {
    public void visit(POReconnect po);

    public void visit(PODataAvailable po);

    public void visit(POException po);

    public void visit(POTimeoutCheck po);

    public void visit(POVersionRequest po);

    public void visit(POAuthenticateRequest po);

    public void visit(POAuthenticateResponse po);

    public void visit(POMetaDataRequest po);

    public void visit(POGetClientIdRequest po);

    public void visit(POSetClientIdRequest po);

    public void visit(PORecreate po);

    public void visit(POHandover po);

    public void visit(POClose po);
}
