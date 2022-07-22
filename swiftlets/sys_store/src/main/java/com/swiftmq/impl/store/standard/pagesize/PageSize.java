/*
 * Copyright 2022 IIT Software GmbH
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

package com.swiftmq.impl.store.standard.pagesize;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;

import java.util.concurrent.atomic.AtomicInteger;

public class PageSize {
    private static final AtomicInteger current = new AtomicInteger();
    private static final AtomicInteger recommended = new AtomicInteger();
    private static Property pageSizeCurrentProp = null;
    private static Property pageSizeRecommendedProp = null;
    private static Property resizeOnStartupProp = null;
    private static Property pageSizeMaxProp = null;

    public static void init(Entity dbEntity) {
        pageSizeCurrentProp = dbEntity.getProperty("page-size-current");
        current.set((Integer) pageSizeCurrentProp.getValue());
        pageSizeRecommendedProp = dbEntity.getProperty("page-size-recommended");
        recommended.set((Integer) pageSizeRecommendedProp.getValue());
        resizeOnStartupProp = dbEntity.getProperty("page-resize-on-startup");
        pageSizeMaxProp = dbEntity.getProperty("page-size-max");
    }

    public static int maxPageSize() {
        return (Integer) pageSizeMaxProp.getValue();
    }

    public static boolean isResizeOnStartup() {
        return (Boolean) resizeOnStartupProp.getValue();
    }

    public static int getCurrent() {
        return current.get();
    }

    public static void setCurrent(int newSize) {
        current.set(newSize);
        try {
            pageSizeCurrentProp.setValue(newSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getRecommended() {
        return recommended.get();
    }

    public static void setRecommended(int newSize) {
        recommended.set(newSize);
        try {
            pageSizeRecommendedProp.setValue(newSize);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
