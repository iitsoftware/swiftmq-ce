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

package amqp.v100.base;

import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPInt;
import com.swiftmq.amqp.v100.types.AMQPString;

import java.util.ArrayList;
import java.util.List;

public class ReceiveVerifier {
    Object caller = null;
    int nMsgs = 0;
    String propName = null;
    int[] msgNumbers = null;
    int lastNo = -1;
    boolean checkSequence = true;
    boolean missingOk = false;
    boolean doublesOk = false;

    public ReceiveVerifier(Object caller, int nMsgs, String propName, boolean doublesOk, boolean misssesOk) {
        this.caller = caller;
        this.nMsgs = nMsgs;
        this.propName = propName;
        this.doublesOk = doublesOk;
        this.missingOk = misssesOk;
        msgNumbers = new int[nMsgs];
    }

    public synchronized void setCheckSequence(boolean checkSequence) {
        this.checkSequence = checkSequence;
    }

    public synchronized void add(AMQPMessage msg) throws Exception {
        int no = ((AMQPInt) msg.getApplicationProperties().getValue().get(new AMQPString(propName))).getValue();
        if (no < 0 || no > msgNumbers.length - 1) {
            throw new Exception(caller + "/MsgNoVerifier: no " + no + " is out of range [0.." + (msgNumbers.length - 1));
        }
        if (checkSequence && no != lastNo + 1) {
            throw new Exception(caller + "/MsgNoVerifier: no " + no + " is out of sequence! Last=" + lastNo);
        }
        msgNumbers[no]++;
        if (msgNumbers[no] > 1 && !doublesOk) {
            throw new Exception(caller + "/MsgNoVerifier: no " + no + " = " + msgNumbers[no]);
        }
        lastNo = no;
    }

    public synchronized void verify() throws Exception {
        List list = new ArrayList();
        boolean doubles = false;
        boolean misses = false;
        for (int i = 0; i < msgNumbers.length; i++) {
            if (msgNumbers[i] != 1)
                list.add(i + " = " + msgNumbers[i] + "\n");
            if (msgNumbers[i] > 1)
                doubles = true;
            if (msgNumbers[i] == 0)
                misses = true;
        }
        if (list.size() > 0) {
            if (missingOk) {
                if (doubles && !doublesOk)
                    throw new Exception("DOUBLES: " + list);
            } else {
                if (misses)
                    throw new Exception("MISSES: " + list);
            }
        }
    }
}
