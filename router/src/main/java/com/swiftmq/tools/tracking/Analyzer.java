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

package com.swiftmq.tools.tracking;

import com.swiftmq.tools.collection.SortedDupsCollection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Analyzer {
    static boolean hasSeqNo = Boolean.valueOf(System.getProperty("hasseqno", "true")).booleanValue();
    static Map root = new TreeMap();

    private static void store(String line) {
        try {
            StringTokenizer t = new StringTokenizer(line, "-");
            int cnt = t.countTokens();
            if (cnt < 6)
                return;
            String time = t.nextToken();
            String prefix = t.nextToken();
            String seqNo = null;
            if (hasSeqNo && cnt == 7)
                seqNo = t.nextToken();
            String destination = t.nextToken();
            String msgId = t.nextToken();
            String callStack = t.nextToken();
            String operation = t.nextToken();
            String id = seqNo != null ? seqNo + "-" + msgId : msgId;
            Collection collection = (Collection) root.get(id);
            if (collection == null) {
                collection = new SortedDupsCollection(new TreeSet());
                root.put(id, collection);
            }
            collection.add(new Entry(time, callStack, operation));
        } catch (Exception e) {
            System.out.println(line);
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            int linesOk = -1;
            if (args.length == 1)
                linesOk = Integer.parseInt(args[0]);
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line = null;
            while ((line = reader.readLine()) != null) {
                store(line);
            }
            reader.close();
            for (Iterator iter2 = root.entrySet().iterator(); iter2.hasNext(); ) {
                Map.Entry e2 = (Map.Entry) iter2.next();
                Collection c = (Collection) e2.getValue();
                if (linesOk != -1 && c.size() != linesOk) {
                    System.out.println(e2.getKey());
                    for (Iterator iter3 = c.iterator(); iter3.hasNext(); ) {
                        System.out.println("    " + iter3.next());
                    }
                }
            }
        } catch (IOException e) {
        }

    }

    private static class Entry implements Comparable {
        String time;
        String callStack;
        String operation;

        public Entry(String time, String callStack, String operation) {
            this.time = time;
            this.callStack = callStack;
            this.operation = operation;
        }

        public int compareTo(Object o) {
            return time.compareTo(((Entry) o).time);
        }

        public String toString() {
            return time + "-" + callStack + " " + operation;
        }
    }
}
