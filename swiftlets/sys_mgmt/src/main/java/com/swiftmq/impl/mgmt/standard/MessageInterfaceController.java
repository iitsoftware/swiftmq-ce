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

package com.swiftmq.impl.mgmt.standard;

import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.mgmt.PropertyChangeException;

public class MessageInterfaceController {
    SwiftletContext ctx = null;
    volatile boolean enabled = false;
    MessageInterfaceListener listener = null;

    public MessageInterfaceController(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        Property prop = ctx.root.getEntity("message-interface").getProperty("enabled");
        enabled = (Boolean) prop.getValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                try {
                    enabled = (Boolean) newValue;
                    if (enabled)
                        createListener();
                    else
                        removeListener(true);
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        if (enabled)
            createListener();
    }

    private void createListener() throws Exception {
        listener = new MessageInterfaceListener(ctx);
        ctx.mgmtSwiftlet.fireEvent(true);
    }

    private void removeListener(boolean fire) {
        if (fire)
            ctx.mgmtSwiftlet.fireEvent(false);
        if (listener != null) {
            listener.close();
            listener = null;
        }
    }

    public void close() {
        removeListener(false);
    }
}
