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

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.jms.MapMessageImpl;

/**
 * Facade to wrap a javax.jms.MapMessage
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class MapMessage extends Message {

    MapMessage(StreamContext ctx) {
        super(ctx, new MapMessageImpl());
    }

    MapMessage(StreamContext ctx, MapMessageImpl _impl) {
        super(ctx, _impl);
    }

  /**
   * return the type of this Message
   *
   * @return "map"
   */
  public String type() {
    return "map";
  }

    /**
     * Returns the body.
     *
     * @return MapBody
     */
    public MapBody body() {
        return new MapBody(this);
    }
}
