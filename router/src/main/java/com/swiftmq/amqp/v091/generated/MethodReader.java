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

package com.swiftmq.amqp.v091.generated;

import com.swiftmq.amqp.v091.types.Coder;
import com.swiftmq.amqp.v091.types.Method;

import java.io.DataInput;
import java.io.IOException;

/**
 * Factory class that creates AMQP methods out of an input stream.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version 091. Generation Date: Thu Apr 12 12:18:24 CEST 2012
 **/

public class MethodReader {
    private static Method createConnectionMethod(int methodId) throws IOException {
        Method method = null;
        switch (methodId) {
            case 10:
                method = new com.swiftmq.amqp.v091.generated.connection.Start();
                break;
            case 11:
                method = new com.swiftmq.amqp.v091.generated.connection.StartOk();
                break;
            case 20:
                method = new com.swiftmq.amqp.v091.generated.connection.Secure();
                break;
            case 21:
                method = new com.swiftmq.amqp.v091.generated.connection.SecureOk();
                break;
            case 30:
                method = new com.swiftmq.amqp.v091.generated.connection.Tune();
                break;
            case 31:
                method = new com.swiftmq.amqp.v091.generated.connection.TuneOk();
                break;
            case 40:
                method = new com.swiftmq.amqp.v091.generated.connection.Open();
                break;
            case 41:
                method = new com.swiftmq.amqp.v091.generated.connection.OpenOk();
                break;
            case 50:
                method = new com.swiftmq.amqp.v091.generated.connection.Close();
                break;
            case 51:
                method = new com.swiftmq.amqp.v091.generated.connection.CloseOk();
                break;
            default:
                throw new IOException("Invalid methodId: " + methodId);
        }
        return method;
    }

    private static Method createChannelMethod(int methodId) throws IOException {
        Method method = null;
        switch (methodId) {
            case 10:
                method = new com.swiftmq.amqp.v091.generated.channel.Open();
                break;
            case 11:
                method = new com.swiftmq.amqp.v091.generated.channel.OpenOk();
                break;
            case 20:
                method = new com.swiftmq.amqp.v091.generated.channel.Flow();
                break;
            case 21:
                method = new com.swiftmq.amqp.v091.generated.channel.FlowOk();
                break;
            case 40:
                method = new com.swiftmq.amqp.v091.generated.channel.Close();
                break;
            case 41:
                method = new com.swiftmq.amqp.v091.generated.channel.CloseOk();
                break;
            default:
                throw new IOException("Invalid methodId: " + methodId);
        }
        return method;
    }

    private static Method createExchangeMethod(int methodId) throws IOException {
        Method method = null;
        switch (methodId) {
            case 10:
                method = new com.swiftmq.amqp.v091.generated.exchange.Declare();
                break;
            case 11:
                method = new com.swiftmq.amqp.v091.generated.exchange.DeclareOk();
                break;
            case 20:
                method = new com.swiftmq.amqp.v091.generated.exchange.Delete();
                break;
            case 21:
                method = new com.swiftmq.amqp.v091.generated.exchange.DeleteOk();
                break;
            case 30:
                method = new com.swiftmq.amqp.v091.generated.exchange.Bind();
                break;
            case 31:
                method = new com.swiftmq.amqp.v091.generated.exchange.BindOk();
                break;
            case 40:
                method = new com.swiftmq.amqp.v091.generated.exchange.Unbind();
                break;
            case 51:
                method = new com.swiftmq.amqp.v091.generated.exchange.UnbindOk();
                break;
            default:
                throw new IOException("Invalid methodId: " + methodId);
        }
        return method;
    }

    private static Method createQueueMethod(int methodId) throws IOException {
        Method method = null;
        switch (methodId) {
            case 10:
                method = new com.swiftmq.amqp.v091.generated.queue.Declare();
                break;
            case 11:
                method = new com.swiftmq.amqp.v091.generated.queue.DeclareOk();
                break;
            case 20:
                method = new com.swiftmq.amqp.v091.generated.queue.Bind();
                break;
            case 21:
                method = new com.swiftmq.amqp.v091.generated.queue.BindOk();
                break;
            case 50:
                method = new com.swiftmq.amqp.v091.generated.queue.Unbind();
                break;
            case 51:
                method = new com.swiftmq.amqp.v091.generated.queue.UnbindOk();
                break;
            case 30:
                method = new com.swiftmq.amqp.v091.generated.queue.Purge();
                break;
            case 31:
                method = new com.swiftmq.amqp.v091.generated.queue.PurgeOk();
                break;
            case 40:
                method = new com.swiftmq.amqp.v091.generated.queue.Delete();
                break;
            case 41:
                method = new com.swiftmq.amqp.v091.generated.queue.DeleteOk();
                break;
            default:
                throw new IOException("Invalid methodId: " + methodId);
        }
        return method;
    }

    private static Method createBasicMethod(int methodId) throws IOException {
        Method method = null;
        switch (methodId) {
            case 10:
                method = new com.swiftmq.amqp.v091.generated.basic.Qos();
                break;
            case 11:
                method = new com.swiftmq.amqp.v091.generated.basic.QosOk();
                break;
            case 20:
                method = new com.swiftmq.amqp.v091.generated.basic.Consume();
                break;
            case 21:
                method = new com.swiftmq.amqp.v091.generated.basic.ConsumeOk();
                break;
            case 30:
                method = new com.swiftmq.amqp.v091.generated.basic.Cancel();
                break;
            case 31:
                method = new com.swiftmq.amqp.v091.generated.basic.CancelOk();
                break;
            case 40:
                method = new com.swiftmq.amqp.v091.generated.basic.Publish();
                break;
            case 50:
                method = new com.swiftmq.amqp.v091.generated.basic.Return();
                break;
            case 60:
                method = new com.swiftmq.amqp.v091.generated.basic.Deliver();
                break;
            case 70:
                method = new com.swiftmq.amqp.v091.generated.basic.Get();
                break;
            case 71:
                method = new com.swiftmq.amqp.v091.generated.basic.GetOk();
                break;
            case 72:
                method = new com.swiftmq.amqp.v091.generated.basic.GetEmpty();
                break;
            case 80:
                method = new com.swiftmq.amqp.v091.generated.basic.Ack();
                break;
            case 90:
                method = new com.swiftmq.amqp.v091.generated.basic.Reject();
                break;
            case 100:
                method = new com.swiftmq.amqp.v091.generated.basic.RecoverAsync();
                break;
            case 110:
                method = new com.swiftmq.amqp.v091.generated.basic.Recover();
                break;
            case 111:
                method = new com.swiftmq.amqp.v091.generated.basic.RecoverOk();
                break;
            case 120:
                method = new com.swiftmq.amqp.v091.generated.basic.Nack();
                break;
            default:
                throw new IOException("Invalid methodId: " + methodId);
        }
        return method;
    }

    private static Method createTxMethod(int methodId) throws IOException {
        Method method = null;
        switch (methodId) {
            case 10:
                method = new com.swiftmq.amqp.v091.generated.tx.Select();
                break;
            case 11:
                method = new com.swiftmq.amqp.v091.generated.tx.SelectOk();
                break;
            case 20:
                method = new com.swiftmq.amqp.v091.generated.tx.Commit();
                break;
            case 21:
                method = new com.swiftmq.amqp.v091.generated.tx.CommitOk();
                break;
            case 30:
                method = new com.swiftmq.amqp.v091.generated.tx.Rollback();
                break;
            case 31:
                method = new com.swiftmq.amqp.v091.generated.tx.RollbackOk();
                break;
            default:
                throw new IOException("Invalid methodId: " + methodId);
        }
        return method;
    }

    private static Method createConfirmMethod(int methodId) throws IOException {
        Method method = null;
        switch (methodId) {
            case 10:
                method = new com.swiftmq.amqp.v091.generated.confirm.Select();
                break;
            case 11:
                method = new com.swiftmq.amqp.v091.generated.confirm.SelectOk();
                break;
            default:
                throw new IOException("Invalid methodId: " + methodId);
        }
        return method;
    }

    public static Method createMethod(DataInput in) throws IOException {
        Method method = null;
        int classId = Coder.readShort(in);
        int methodId = Coder.readShort(in);

        switch (classId) {
            case 10:
                method = createConnectionMethod(methodId);
                break;
            case 20:
                method = createChannelMethod(methodId);
                break;
            case 40:
                method = createExchangeMethod(methodId);
                break;
            case 50:
                method = createQueueMethod(methodId);
                break;
            case 60:
                method = createBasicMethod(methodId);
                break;
            case 90:
                method = createTxMethod(methodId);
                break;
            case 85:
                method = createConfirmMethod(methodId);
                break;
            default:
                throw new IOException("Invalid classId: " + classId);
        }
        method.readContent(in);
        return method;
    }
}
