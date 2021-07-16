/*
 * Copyright 2020 IIT Software GmbH
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

package com.swiftmq;

public class IsJava15OrLaterWithoutGraalVM {
    public static void main(String[] args) {
        try {
            Runtime.Version version = Runtime.version();
            System.out.print(version.feature() >= 15 && !org.graalvm.home.Version.getCurrent().isRelease());
        } catch (Error e) {
            System.out.print(false);
        }
    }
}
