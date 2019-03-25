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

package com.swiftmq.impl.mgmt.standard.jmx;

import com.swiftmq.impl.mgmt.standard.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.util.SwiftUtilities;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;

public class JMXUtil {
    private static final String DOMAIN = "com.swiftmq.";
    SwiftletContext ctx = null;
    MBeanServer mbs = null;
    boolean groupNames = false;

    public JMXUtil(SwiftletContext ctx, boolean groupNames) {
        this.ctx = ctx;
        this.groupNames = groupNames;
        mbs = getMBeanServer();
    }

    private MBeanServer getMBeanServer() {
        MBeanServer mbeanServer = null;
        String option = (String) ctx.root.getEntity("jmx").getEntity("mbean-server").getProperty("usage-option").getValue();
        if (option.equals("use-platform-server"))
            mbeanServer = ManagementFactory.getPlatformMBeanServer();
        else if (option.equals("use-named-server")) {
            String serverName = (String) ctx.root.getEntity("jmx").getEntity("mbean-server").getProperty("server-name").getValue();
            ArrayList list = MBeanServerFactory.findMBeanServer(null);
            if (list != null && list.size() > 0) {
                for (int i = 0; i < list.size(); i++) {
                    MBeanServer srv = (MBeanServer) list.get(i);
                    if (srv.getDefaultDomain().equals(serverName)) {
                        mbeanServer = srv;
                        break;
                    }
                }
            }
            if (mbeanServer == null) {
                ctx.logSwiftlet.logWarning(ctx.mgmtSwiftlet.getName(), "Unable to find MBean Server '" + (String) ctx.root.getEntity("jmx").getEntity("mbean-server").getProperty("server-name").getValue() + "'. Create the MBean Server with this name!");
                mbeanServer = MBeanServerFactory.createMBeanServer((String) ctx.root.getEntity("jmx").getEntity("mbean-server").getProperty("server-name").getValue());
            }
        } else if (option.equals("create-named-server"))
            mbeanServer = MBeanServerFactory.createMBeanServer((String) ctx.root.getEntity("jmx").getEntity("mbean-server").getProperty("server-name").getValue());
        return mbeanServer;
    }

    public void setGroupNames(boolean groupNames) {
        this.groupNames = groupNames;
    }

    public static String toJMXString(String s) {
        return s.replace(':', '_').replace('=', '_').replace(' ', '_');
    }

    public String getContext(Entity entity) {
        String res = null;
        String[] econtext = entity.getContext();
        String context = toJMXString(SwiftletManager.getInstance().getRouterName()) + ":";
        if (groupNames) {
            StringBuffer b = new StringBuffer();
            for (int i = 0; i < econtext.length; i++) {
                if (i > 0)
                    b.append(", ");
                if (i > 0) {
                    b.append("name");
                    b.append(i);
                } else
                    b.append("type");
                b.append("=");
                b.append(toJMXString(econtext[i]));
            }
            res = context + b.toString();
        } else
            res = context + "type=" + toJMXString(SwiftUtilities.concat(econtext, "/"));
        return res;
    }

    public void registerMBean(EntityMBean mbean) {
        try {
            ObjectName name = new ObjectName(DOMAIN + getContext(mbean.getEntity()));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/registerMBean, name=" + name);
            mbean.setObjectName(name);
            mbs.registerMBean(mbean, name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void unregisterMBean(EntityMBean mbean) {
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/unregisterMBean, name=" + mbean.getObjectName());
            mbs.unregisterMBean(mbean.getObjectName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String toString() {
        return "JMXUtil";
    }
}
