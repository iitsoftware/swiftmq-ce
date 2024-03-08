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
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.tools.sql.LikeComparator;
import com.swiftmq.util.SwiftUtilities;

import java.util.List;
import java.util.Map;

public class AuthenticatorImpl implements Authenticator {
    SwiftletContext ctx = null;
    Role role = null;

    public AuthenticatorImpl(SwiftletContext ctx, Role role) {
        this.ctx = ctx;
        this.role = role;
    }

    private boolean isIncluded(String[] array, String s) {
        for (String value : array) {
            if (LikeComparator.compare(s, value, '\\'))
                return true;
        }
        return false;
    }

    private boolean contextCheck(Entity entity) {
        if (role == null)
            return true;
        String context = buildContextString(entity);
        boolean granted = role.isContextGranted(context, false);
        Map entities = entity.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Object o : entities.entrySet()) {
                Entity e = (Entity) ((Map.Entry<?, ?>) o).getValue();
                if (contextCheck(e)) {
                    granted = true;
                    break;
                }
            }
        }
        Map props = entity.getProperties();
        if (props != null && props.size() > 0) {
            for (Object o : props.entrySet()) {
                Property prop = (Property) ((Map.Entry<?, ?>) o).getValue();
                if (role.isContextGranted(context + "/" + prop.getName(), false)) {
                    granted = true;
                    break;
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/contextCheck, context " + context + ", granted = " + granted);
        return granted;
    }

    private String buildContextString(Entity entity) {
        String[] c = entity == null ? null : entity.getContext();
        if (c == null)
            return "/" + SwiftletManager.getInstance().getRouterName();
        return "/" + SwiftUtilities.concat(entity.getContext(), "/");
    }

    public boolean roleStrip(Entity entity) throws Exception {
        if (role == null)
            return true;
        String context = buildContextString(entity);
        boolean granted = role.isContextGranted(context, false);
        Map entities = entity.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Object o : entities.entrySet()) {
                Entity e = (Entity) ((Map.Entry<?, ?>) o).getValue();
                if (!roleStrip(e)) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/roleStrip, remove entity: " + e.getName());
                    entity.removeEntity(e);
                } else {
                    granted = true;
                }
            }
        }
        Map props = entity.getProperties();
        if (props != null && props.size() > 0) {
            for (Object o : props.entrySet()) {
                Property prop = (Property) ((Map.Entry) o).getValue();
                if (!role.isContextGranted(context + "/" + prop.getName(), false)) {
                    entity.removeProperty(prop.getName());
                    Entity parent = entity.getParent();
                    if (parent instanceof EntityList) {
                        ((EntityList) parent).getTemplate().removeProperty(prop.getName());
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/roleStrip, remove template property: " + prop.getName());
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/roleStrip, remove property: " + prop.getName());
                } else {
                    granted = true;
                    if (role.isContextGrantedAndReadOnly(context + "/" + prop.getName()))
                        prop.setReadOnly(true);
                }
            }
        }
        if (granted) {
            CommandRegistry commandRegistry = entity.getCommandRegistry();
            String[] grantedCommands = role.getGrantedCommands(context);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/roleStrip, context: " + context + ", granted commands: " + SwiftUtilities.concat(grantedCommands, ", "));
            List cmdList = commandRegistry.getCommands();
            for (Object o : cmdList) {
                Command cmd = (Command) o;
                boolean inc = isIncluded(grantedCommands, cmd.getName());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/roleStrip, context: " + context + ", command: " + cmd.getName() + " included: " + inc);
                cmd.setEnabled(inc);
                if (!inc)
                    cmd.setGuiEnabled(false);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/roleStrip, context " + context + ", granted = " + granted);
        return granted;
    }

    public boolean isContextGranted(Entity entity) {
        if (role == null)
            return true;
        Entity e = entity;
        if (entity == null)
            e = RouterConfiguration.Singleton();
        boolean granted = contextCheck(e);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isContextGranted, context=" + e.getContext() + ", granted=" + granted);
        return granted;
    }

    public boolean isCommandGranted(Entity entity, String cmd) {
        if (role == null)
            return true;
        Entity e = entity;
        if (entity == null)
            e = RouterConfiguration.Singleton();
        boolean granted = contextCheck(e);
        String context = buildContextString(e);
        if (granted) {
            String[] grantedCommands = role.getGrantedCommands(context);
            granted = isIncluded(grantedCommands, cmd);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isCommandGranted, context: " + context + ", granted commands: " + SwiftUtilities.concat(grantedCommands, ", "));
        }
        if (!granted) {
            Map entities = e.getEntities();
            if (entities != null && entities.size() > 0) {
                for (Object o : entities.entrySet()) {
                    Entity sub = (Entity) ((Map.Entry) o).getValue();
                    if (isCommandGranted(sub, cmd)) {
                        granted = true;
                        break;
                    }
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isCommandGranted, context=" + context + ", cmd=" + cmd + ", granted=" + granted);
        return granted;
    }

    public boolean isPropertyGranted(Entity entity, String name) {
        if (role == null)
            return true;
        String context = "/" + SwiftUtilities.concat(entity.getContext(), "/") + "/" + name;
        boolean granted = role.isContextGranted(context, false);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isPropertyGranted, context=" + context + ", granted=" + granted);
        return granted;
    }

    public boolean isPropertyReadOnly(Entity entity, String name) {
        if (role == null)
            return true;
        String context = "/" + SwiftUtilities.concat(entity.getContext(), "/") + "/" + name;
        boolean granted = role.isContextGranted(context, true);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isPropertyChangeGranted, context=" + context + ", granted=" + granted);
        return granted;
    }

    @Override
    public String toString() {
        return "[AuthenticatorImpl, " +
                "role=" + role +
                ']';
    }
}
