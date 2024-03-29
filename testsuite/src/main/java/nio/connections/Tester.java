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

package nio.connections;

import jms.base.PTPTestCase;

import javax.jms.JMSException;
import javax.jms.QueueConnection;

public class Tester extends PTPTestCase {
    public Tester(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void test() {
        try {
            for (int j = 0; j < 10; j++) {
                QueueConnection[] qc = new QueueConnection[5];
                for (int i = 0; i < qc.length; i++)
                    qc[i] = createQueueConnection("plainsocket@router");
                for (int i = 0; i < qc.length; i++)
                    qc[i].start();
                for (int i = 0; i < qc.length; i++)
                    qc[i].close();
            }
        } catch (JMSException e) {
            fail("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
