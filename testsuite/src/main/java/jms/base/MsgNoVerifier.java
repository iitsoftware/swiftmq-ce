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

package jms.base;

import javax.jms.Message;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MsgNoVerifier {
    Object caller = null;
    int nMsgs = 0;
    String propName = null;
    int[] msgNumbers = null;
    int lastNo = -1;
    boolean checkSequence = true;
    boolean missingOk = false;
    boolean doublesOk = false;

    public MsgNoVerifier(Object caller, int nMsgs, String propName) {
        this.caller = caller;
        this.nMsgs = nMsgs;
        this.propName = propName;
        msgNumbers = new int[nMsgs];
    }

    public MsgNoVerifier(Object caller, int nMsgs, String propName, boolean doublesOk) {
        this.caller = caller;
        this.nMsgs = nMsgs;
        this.propName = propName;
        this.doublesOk = doublesOk;
        msgNumbers = new int[nMsgs];
    }

    public String shootThreadDump() {
        StringWriter writer = new StringWriter();
        Map st = Thread.getAllStackTraces();
        for (Iterator iter = st.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry e = (Map.Entry) iter.next();
            StackTraceElement[] el = (StackTraceElement[]) e.getValue();
            Thread t = (Thread) e.getKey();
            writer.write("\"" + t.getName() + "\"" + " " +
                    (t.isDaemon() ? "daemon" : "") + " prio=" + t.getPriority() +
                    " Thread id=" + t.getId() + " " + t.getState() + "\n");
            for (int i = 0; i < el.length; i++) {
                writer.write("\t" + el[i] + "\n");
            }
            writer.write("\n");
        }
        return writer.toString();
    }

    public synchronized void setCheckSequence(boolean checkSequence) {
        this.checkSequence = checkSequence;
    }

    public synchronized void setMissingOk(boolean missingOk) {
        this.missingOk = missingOk;
    }

    public synchronized void setDoublesOk(boolean doublesOk) {
        this.doublesOk = doublesOk;
    }

    public synchronized void add(Message msg) throws Exception {
        int no = msg.getIntProperty(propName);
        if (no < 0 || no > msgNumbers.length - 1) {
            throw new Exception(caller + "/MsgNoVerifier: no " + no + " is out of range [0.." + (msgNumbers.length - 1));
        }
        if (checkSequence && no != lastNo + 1) {
            System.out.println(caller + "/MsgNoVerifier: no " + no + " is out of sequence! Last=" + lastNo);
            System.exit(-1);
        }
        msgNumbers[no]++;
        if (msgNumbers[no] > 1 && !doublesOk) {
            System.out.println(caller + "/MsgNoVerifier: no " + no + " = " + msgNumbers[no]);
            System.exit(-1);
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
                if (doubles) {
                    System.out.println(caller + "/MsgNoVerifier:\n" + list);
                    if (!doublesOk)
                        System.exit(-1);
                }
            } else {
                String dump = !misses ? "" : "Thread Dump:\n" + shootThreadDump() + "\n";
                System.out.println(caller + "/MsgNoVerifier:\n" + (!misses ? "" : dump) + list);
                System.exit(-1);
            }
        }
    }
}
