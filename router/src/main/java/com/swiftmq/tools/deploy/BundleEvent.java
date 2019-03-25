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

package com.swiftmq.tools.deploy;

public class BundleEvent {
    final static int BUNDLE_UNCHANGED = -1;
    public final static int BUNDLE_ADDED = 0;
    public final static int BUNDLE_REMOVED = 1;
    public final static int BUNDLE_CHANGED = 2;

    int type;
    Bundle bundle;

    /**
     * @param type
     * @param bundle
     * @SBGen Constructor assigns type, bundle
     */
    BundleEvent(int type, Bundle bundle) {
        // SBgen: Assign variables
        this.type = type;
        this.bundle = bundle;
        // SBgen: End assign
    }


    /**
     * @return
     * @SBGen Method get type
     */
    public int getType() {
        // SBgen: Get variable
        return (type);
    }

    /**
     * @return
     * @SBGen Method get bundle
     */
    public Bundle getBundle() {
        // SBgen: Get variable
        return (bundle);
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[BundleEvent, type=");
        switch (type) {
            case BUNDLE_UNCHANGED:
                b.append("BUNDLE_UNCHANGED");
                break;
            case BUNDLE_ADDED:
                b.append("BUNDLE_ADDED");
                break;
            case BUNDLE_REMOVED:
                b.append("BUNDLE_REMOVED");
                break;
            case BUNDLE_CHANGED:
                b.append("BUNDLE_CHANGED");
                break;
        }
        b.append(", bundle=");
        b.append(bundle.toString());
        b.append("]");
        return b.toString();
    }
}

