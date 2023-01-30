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
import com.swiftmq.tools.sql.LikeComparator;

import java.util.Arrays;

public class AnnounceFilters {
    TopicManagerContext ctx;

    public AnnounceFilters(TopicManagerContext ctx) {
        this.ctx = ctx;
    }

    private Entity getRouterFilter(String routername) {
        Entity result = null;
        if (ctx.announceFilterList != null) {
            String[] names = ctx.announceFilterList.getEntityNames();
            if (names != null) {
                String name = Arrays.asList(names).stream().filter(s -> LikeComparator.compare(routername, s, '\\')).findFirst().orElse(null);
                if (name != null)
                    result = ctx.announceFilterList.getEntity(name);
            }
        }
        return result;
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
        String[] names = routerFilter.getEntity("topic-filters").getEntityNames();
        if (names != null) {
            String name = Arrays.asList(names).stream().filter(s -> LikeComparator.compare(topicname, s, '\\')).findFirst().orElse(null);
            if (name != null)
                enabled = type.equals("include");
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), "AnnounceFilter/routername=" + routername + ", topicname=" + topicname + ", enabled=" + enabled);
        return enabled;
    }
}
