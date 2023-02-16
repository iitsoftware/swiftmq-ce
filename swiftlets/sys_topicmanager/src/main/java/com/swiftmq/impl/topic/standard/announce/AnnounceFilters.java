/*
 * Copyright 2023 IIT Software GmbH
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

package com.swiftmq.impl.topic.standard.announce;

import com.swiftmq.impl.topic.standard.TopicManagerContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.tools.sql.LikeComparator;

public class AnnounceFilters {
    TopicManagerContext ctx;

    public AnnounceFilters(TopicManagerContext ctx) {
        this.ctx = ctx;
    }

    private Entity findEntity(EntityList list, String name, String propName) {
        if (list != null) {
            String[] names = list.getEntityNames();
            if (names != null) {
                for (String s : names) {
                    Entity entity = list.getEntity(s);
                    if (LikeComparator.compare(name, propName.equals("name") ? entity.getName() : entity.getProperty(propName).getValue().toString(), '\\'))
                        return entity;
                }
            }
        }
        return null;
    }

    private Entity getRouterFilter(String routername) {
        return findEntity(ctx.announceFilterList, routername, "routername");
    }

    public boolean isAnnounceEnabled(String routername, String topicname) {
        if (topicname.equals("swiftmq")) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), "AnnounceFilter/routername=" + routername + ", topicname=" + topicname + ", always enabled");
            return true;
        }
        Entity routerFilter = getRouterFilter(routername);
        if (routerFilter == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), "AnnounceFilter/routername=" + routername + ", topicname=" + topicname + ", enabled (no filter)");
            return true;
        }
        String type = (String) routerFilter.getProperty("filter-type").getValue();
        boolean enabled = !type.equals("include");
        Entity entity = findEntity((EntityList) routerFilter.getEntity("topic-filters"), topicname, "name");
        if (entity != null)
            enabled = type.equals("include");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), "AnnounceFilter/routername=" + routername + ", topicname=" + topicname + ", enabled=" + enabled);
        return enabled;
    }
}
