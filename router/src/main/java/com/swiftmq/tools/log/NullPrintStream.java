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

package com.swiftmq.tools.log;

import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.PrintStream;

public class NullPrintStream extends PrintStream {
    public NullPrintStream() {
        super(new DataByteArrayOutputStream(10));
    }

    public void flush() {
    }

    public void close() {
    }

    public void print(boolean b) {
    }

    public void print(char c) {
    }

    public void print(int i) {
    }

    public void print(long l) {
    }

    public void print(float f) {
    }

    public void print(double d) {
    }

    public void print(char s[]) {
    }

    public void print(String s) {
    }

    public void print(Object obj) {
    }

    public void println() {
    }

    public void println(boolean x) {
    }

    public void println(char x) {
    }

    public void println(int x) {
    }

    public void println(long x) {
    }

    public void println(float x) {
    }

    public void println(double x) {
    }

    public void println(char x[]) {
    }

    public void println(String x) {
    }

    public void println(Object x) {
    }
}
