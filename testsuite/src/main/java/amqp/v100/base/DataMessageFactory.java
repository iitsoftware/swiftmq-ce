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

import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;

import java.util.List;

public class DataMessageFactory implements MessageFactory {
    static byte[] b = new byte[1024];

    public AMQPMessage create(int sequenceNo) throws Exception {
        AMQPMessage msg = new AMQPMessage();
        msg.addData(new Data(b));
        msg.addData(new Data(b));
        msg.addData(new Data(b));
        return msg;
    }

    public void verify(AMQPMessage message) throws Exception {
        List data = message.getData();
        if (data == null)
            throw new Exception(("verify - no Data section found!"));
        int total = 0;
        for (int i = 0; i < data.size(); i++)
            total += ((Data) data.get(i)).getValue().length;
        if (total != 1024 * 3)
            throw new Exception("verify - expected " + (3 * 1024) + " but got " + total);
    }

    public AMQPMessage createReplyMessage(AMQPMessage request) throws Exception {
        AMQPMessage reply = new AMQPMessage();
        List data = request.getData();
        for (int i = 0; i < data.size(); i++)
            reply.addData((Data) data.get(i));
        return reply;
    }
}
