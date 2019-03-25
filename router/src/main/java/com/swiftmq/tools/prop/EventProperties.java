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

package com.swiftmq.tools.prop;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

/**
 * EventProperties adds PropertyChangeSupport to the Properties class. Adding, removing,
 * and value change on properties will be propagated on all registered listeners. The propagation
 * will also take place on load, so this class could be an initializer for an application, based on properties!
 *
 * @Author Andreas Mueller, IIT GmbH
 * @Version 1.0
 */
public class EventProperties extends Properties {
    Hashtable propListeners = new Hashtable();
    PropertyChangeListener generalListener = null;

    /**
     * @SBGen Constructor
     */
    public EventProperties(Properties defaults) {
        super(defaults);
    }


    /**
     * @SBGen Constructor
     */
    public EventProperties() {
        this(null);
    }

    public Object setProperty(String key, String value) {
        String oldValue = getProperty(key);
        Object obj = super.setProperty(key, value);
        firePropertyChange(key, oldValue, value);
        return obj;
    }

    public Object remove(Object key) {
        String skey = (String) key;
        String oldValue = getProperty(skey);
        Object obj = super.remove(skey);
        firePropertyChange(skey, oldValue, null);
        return obj;
    }

    public void load(InputStream inStream)
            throws IOException {
        Properties p = new Properties();
        p.load(inStream);
        // check on new and changed props
        Enumeration newProps = p.propertyNames();
        while (newProps.hasMoreElements()) {
            String key = (String) newProps.nextElement();
            String newValue = p.getProperty(key);
            setProperty(key, newValue);
        }
    }

    public void addPropertyChangeListener(String key, PropertyChangeListener listener) {
        synchronized (propListeners) {
            if (key == null)
                generalListener = listener;
            else {
                Vector vec = (Vector) propListeners.get(key);
                if (vec != null)
                    vec.addElement(listener);
                else {
                    vec = new Vector();
                    vec.addElement(listener);
                    propListeners.put(key, vec);
                }
            }
        }
    }

    public void removePropertyChangeListener(String key, PropertyChangeListener listener) {
        synchronized (propListeners) {
            if (key == null)
                generalListener = null;
            else {
                Vector vec = (Vector) propListeners.get(key);
                if (vec != null) {
                    vec.removeElement(listener);
                    if (vec.size() == 0)
                        propListeners.remove(key);
                }
            }
        }
    }

    private void firePropertyChange(String key, String oldValue, String newValue) {
        if (oldValue == null && newValue != null ||
                oldValue != null && newValue == null ||
                oldValue != null && newValue != null && !oldValue.equals(newValue)) {
            synchronized (propListeners) {
                Vector vec = (Vector) propListeners.get(key);
                if (vec != null) {
                    PropertyChangeEvent evt = new PropertyChangeEvent(this, key, oldValue, newValue);
                    for (int i = 0; i < vec.size(); i++)
                        ((PropertyChangeListener) vec.elementAt(i)).propertyChange(evt);
                }
                if (generalListener != null)
                    generalListener.propertyChange(new PropertyChangeEvent(this, key, oldValue, newValue));
            }
        }
    }
}

