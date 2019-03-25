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

package com.swiftmq.impl.trace.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.trace.TraceSpace;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

public class TraceSpaceImpl extends TraceSpace {
    static SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S/");
    Entity spaceEntity = null;
    EntityList predicateList = null;

    public TraceSpaceImpl(String name, boolean enabled) {
        super(name, enabled);
    }

    public TraceSpaceImpl(Entity spaceEntity, EntityList predicateList) {
        super(spaceEntity.getName(), ((Boolean) spaceEntity.getProperty("enabled").getValue()).booleanValue());
        this.predicateList = predicateList;
        Property prop = spaceEntity.getProperty("enabled");
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property prop, Object oldValue, Object newValue) throws PropertyChangeException {
                enabled = ((Boolean) newValue).booleanValue();
            }
        });
    }

    public void trace(String subEntity, String message) {
        if (!enabled)
            return;
        if (predicateList == null)
            return;
        Calendar cal = Calendar.getInstance();
        StringBuffer outline = new StringBuffer();
        outline.append(fmt.format(cal.getTime()));
        outline.append(getSpaceName());
        outline.append("/");
        if (subEntity != null) {
            outline.append(subEntity.toString());
            outline.append("/");
        }
        outline.append(message);
        String s = outline.toString();
        Map entities = predicateList.getEntities();
        if (entities != null) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
                TraceDestination dest = (TraceDestination) entity.getUserObject();
                if (dest != null)
                    dest.trace(subEntity, s);
            }
        }
    }

    protected void closeSpaceResources() {
    }
}
