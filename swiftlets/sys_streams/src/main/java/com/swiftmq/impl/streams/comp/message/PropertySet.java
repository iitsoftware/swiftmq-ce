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

package com.swiftmq.impl.streams.comp.message;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Facade to wrap a Set of Properties of a javax.jms.Message
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class PropertySet {
    Message message;
    Map<String, Property> properties;

    PropertySet(Message message, Map<String, Property> properties) {
        this.message = message;
        this.properties = properties;
    }

  /**
   * Returns a new PropertySet that contains all Properties which name is not equals the prefix.
   *
   * @param prefix Prefix
   * @return PropertySet
   */
  public PropertySet notEquals(final String prefix) {
    final Map<String, Property> result = new HashMap<String, Property>();
    forEach(new ForEachPropertyCallback() {
      @Override
      public void execute(Property property) {
        if (!property.name().equals(prefix))
          result.put(property.name(), property);
      }
    });
    return new PropertySet(message, result);
  }

  /**
   * Returns a new PropertySet that contains all Properties which name starts with the prefix.
   *
   * @param prefix Prefix
   * @return PropertySet
   */
  public PropertySet startsWith(final String prefix) {
    final Map<String, Property> result = new HashMap<String, Property>();
    forEach(new ForEachPropertyCallback() {
      @Override
      public void execute(Property property) {
        if (property.name().startsWith(prefix))
          result.put(property.name(), property);
      }
    });
    return new PropertySet(message, result);
  }

    /**
     * Returns a new PropertySet that contains all Properties which name do NOT starts with the prefix.
     *
     * @param prefix Prefix
     * @return PropertySet
     */
    public PropertySet notStartsWith(final String prefix) {
        final Map<String, Property> result = new HashMap<String, Property>();
        forEach(new ForEachPropertyCallback() {
            @Override
            public void execute(Property property) {
                if (!property.name().startsWith(prefix))
                    result.put(property.name(), property);
            }
        });
        return new PropertySet(message, result);
    }

    /**
     * Returns a new PropertySet that contains all Properties which name ends with the suffix.
     *
     * @param suffix Suffix
     * @return PropertySet
     */
    public PropertySet endsWith(final String suffix) {
        final Map<String, Property> result = new HashMap<String, Property>();
        forEach(new ForEachPropertyCallback() {
            @Override
            public void execute(Property property) {
                if (property.name().endsWith(suffix))
                    result.put(property.name(), property);
            }
        });
        return new PropertySet(message, result);
    }

    /**
     * Returns a new PropertySet that contains all Properties which name does NOT end with the suffix.
     *
     * @param suffix Suffix
     * @return PropertySet
     */
    public PropertySet notEndsWith(final String suffix) {
        final Map<String, Property> result = new HashMap<String, Property>();
        forEach(new ForEachPropertyCallback() {
            @Override
            public void execute(Property property) {
                if (!property.name().endsWith(suffix))
                    result.put(property.name(), property);
            }
        });
        return new PropertySet(message, result);
    }

    /**
     * Returns a new PropertySet that contains all Properties which name matches the regular expression.
     *
     * @param regex Regular Expression
     * @return PropertySet
     */
    public PropertySet select(String regex) {
        final Pattern pattern = Pattern.compile(regex);
        final Map<String, Property> result = new HashMap<String, Property>();
        forEach(new ForEachPropertyCallback() {
            @Override
            public void execute(Property property) {
                Matcher matcher = pattern.matcher(property.name());
                if (matcher.find())
                    result.put(property.name(), property);
            }
        });
        return new PropertySet(message, result);
    }

    /**
     * Invokes the callback for each Property of this PropertySet
     *
     * @param callback Callback
     */
    public void forEach(ForEachPropertyCallback callback) {
        for (Iterator<Map.Entry<String, Property>> iter = properties.entrySet().iterator(); iter.hasNext(); )
            callback.execute(iter.next().getValue());
    }
}
