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

package com.swiftmq.impl.streams;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;

/**
 * Encapsulates the Parameters for a Script
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class Parameters {
    EntityList entityList;

    Parameters(EntityList entityList) {
        this.entityList = entityList;
    }

    /**
     * Returns the parameter value for this name.
     *
     * @param name parameter name
     * @return value or null if not defined
     */
    public String get(String name) {
        Entity entity = entityList.getEntity(name);
        if (entity == null)
            return null;
        return (String) entity.getProperty("value").getValue();
    }

    /**
     * Returns a required paremeter with this name. Throws an exception if not defined.
     *
     * @param name parameter name
     * @return value
     * @throws Exception if not defined
     */
    public String require(String name) throws Exception {
        String value = get(name);
        if (value == null)
            throw new Exception("Missing parameter: " + name);
        return value;
    }

    /**
     * Returns an optional parameter. Returns the default value if not defined.
     *
     * @param name         parameter name
     * @param defaultValue default value
     * @return value
     */
    public String optional(String name, String defaultValue) {
        String value = get(name);
        return value == null ? defaultValue : value;
    }
}
