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

package com.swiftmq.ms.artemis;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.Selector;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.selector.filter.BooleanExpression;
import org.apache.activemq.artemis.selector.filter.FilterException;
import org.apache.activemq.artemis.selector.filter.Filterable;
import org.apache.activemq.artemis.selector.impl.SelectorParser;

import javax.jms.InvalidSelectorException;

public class MessageSelector implements Selector, Filterable {
    String conditionString;
    MessageImpl current;

    public MessageSelector(String conditionString) {
        this.conditionString = conditionString;
    }

    @Override
    public String getConditionString() {
        return conditionString;
    }

    public void compile() throws InvalidSelectorException {
        try {
            SelectorParser.parse(conditionString);
        } catch (Throwable t) {
            String s = t.getMessage();
            if (s == null || s.length() == 0)
                s = "Invalid selector";
            throw new InvalidSelectorException("<" + conditionString + ">: " + s);
        }
    }

    @Override
    public boolean isSelected(MessageImpl message) {
        this.current = message;
        try {
            BooleanExpression selector = SelectorParser.parse(conditionString);
            if (selector != null)
                return selector.matches(this);
        } catch (FilterException e) {
            return false;
        }
        return false;
    }

    @Override
    public <T> T getBodyAs(Class<T> aClass) throws FilterException {
        return null;
    }

    @Override
    public Object getProperty(SimpleString simpleString) {
        return current.getField(simpleString.toString());
    }

    @Override
    public Object getLocalConnectionId() {
        return null;
    }
}
