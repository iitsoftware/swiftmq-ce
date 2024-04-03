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

package com.swiftmq.impl.mgmt.standard.auth;

import com.swiftmq.impl.mgmt.standard.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.tools.sql.LikeComparator;
import com.swiftmq.util.SwiftUtilities;

import java.util.Map;

public class Role {
    SwiftletContext ctx = null;
    Entity roleEntity = null;
    Property adminRolesEnabled = null;

    public Role(SwiftletContext ctx, Entity roleEntity) {
        this.ctx = ctx;
        this.roleEntity = roleEntity;
        this.adminRolesEnabled = ctx.root.getProperty("admin-roles-enabled");
    }

    public boolean isContextGranted(String context, boolean wantChange) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isContextGranted, context: " + context + ", wantChange: " + wantChange);
        if (!((Boolean) adminRolesEnabled.getValue()).booleanValue())
            return true;
        boolean granted = false;
        EntityList filters = (EntityList) roleEntity.getEntity("context-filters");
        if (filters != null) {
            Map map = filters.getEntities();
            if (map != null && map.size() > 0) {
                boolean wasIncludeGranted = false;
                for (Object o : map.entrySet()) {
                    Entity filter = (Entity) ((Map.Entry<?, ?>) o).getValue();
                    String predicate = (String) filter.getProperty("cli-context-predicate").getValue();
                    String type = (String) filter.getProperty("type").getValue();
                    boolean readOnly = (Boolean) filter.getProperty("read-only").getValue();
                    if (LikeComparator.compare(context, predicate, '\\')) {
                        if (type.equals("include")) {
                            granted = true;
                            wasIncludeGranted = true;
                            if (wantChange && readOnly)
                                granted = false;
                        } else {
                            granted = false;
                            wasIncludeGranted = false;
                        }
                    } else {
                        if (!wasIncludeGranted)
                            granted = false;
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isContextGranted, context: " + context + ", predicate: " + predicate + ", type: " + type + ", readOnly: " + readOnly + ", granted=" + granted);
                }
            }
        }
        return granted;
    }

    public String[] getGrantedCommands(String context) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/getGrantedCommands, context: " + context);
        EntityList filters = (EntityList) roleEntity.getEntity("context-filters");
        if (filters != null) {
            Map map = filters.getEntities();
            if (map != null && map.size() > 0) {
                for (Object o : map.entrySet()) {
                    Entity filter = (Entity) ((Map.Entry<?, ?>) o).getValue();
                    String predicate = (String) filter.getProperty("cli-context-predicate").getValue();
                    if (LikeComparator.compare(context, predicate, '\\')) {
                        String cmds = (String) filter.getProperty("granted-commands").getValue();
                        if (cmds != null)
                            return SwiftUtilities.tokenize(cmds, " ,");
                    }
                }
            }
        }
        return new String[0];
    }

    public boolean isContextGrantedAndReadOnly(String context) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isContextGrantedAndReadOnly, context: " + context);
        boolean granted = false;
        boolean ro = false;
        EntityList filters = (EntityList) roleEntity.getEntity("context-filters");
        if (filters != null) {
            Map map = filters.getEntities();
            if (map != null && map.size() > 0) {
                boolean wasIncludeGranted = false;
                for (Object o : map.entrySet()) {
                    Entity filter = (Entity) ((Map.Entry<?, ?>) o).getValue();
                    String predicate = (String) filter.getProperty("cli-context-predicate").getValue();
                    String type = (String) filter.getProperty("type").getValue();
                    boolean readOnly = (Boolean) filter.getProperty("read-only").getValue();
                    if (LikeComparator.compare(context, predicate, '\\')) {
                        if (type.equals("include")) {
                            granted = true;
                            wasIncludeGranted = true;
                            ro = readOnly;
                        } else {
                            granted = false;
                            wasIncludeGranted = false;
                        }
                    } else {
                        if (!wasIncludeGranted)
                            granted = false;
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isContextGrantedAndReadOnly, predicate: " + predicate + ", type: " + type + ", readOnly: " + readOnly + ", granted=" + granted);
                }
            }
        }
        return granted && ro;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[Role");
        sb.append(" name=").append(roleEntity.getName());
        sb.append(']');
        return sb.toString();
    }
}
