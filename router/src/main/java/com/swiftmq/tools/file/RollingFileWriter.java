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

package com.swiftmq.tools.file;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

public class RollingFileWriter extends Writer {
  static SimpleDateFormat FMTROTATE = new SimpleDateFormat("'-'yyyyMMddHHmmss'.old'");
  String filename = null;
  File file = null;
  String directory = null;
  FileWriter writer = null;
  long length = 0;
  int generation = 0;
  RolloverSizeProvider rolloverSizeProvider = null;
  NumberGenerationProvider numberGenerationProvider = null;

  public RollingFileWriter(String filename, RolloverSizeProvider rolloverSizeProvider, NumberGenerationProvider numberGenerationProvider) throws IOException {
    this(filename, rolloverSizeProvider);
    this.numberGenerationProvider = numberGenerationProvider;
  }

  public RollingFileWriter(String filename, RolloverSizeProvider rolloverSizeProvider) throws IOException {
    this.filename = filename;
    this.rolloverSizeProvider = rolloverSizeProvider;
    file = new File(filename);
    directory = file.getParent();
    if (file.exists())
      length = file.length();
    writer = new FileWriter(filename, true);
  }

  private void checkGenerations() throws IOException
  {
    int ngen = numberGenerationProvider.getNumberGenerations();
    if (ngen <= 0)
      return;
    File dir = new File(directory);
    if (!dir.exists()) {
      dir.mkdir();
      return;
    }
    final String fn = file.getName();
    String[] names = dir.list(new FilenameFilter() {
      public boolean accept(File file, String name) {
        return name.startsWith(fn) && name.endsWith(".old");
      }
    });
    if (names != null) {
      Arrays.sort(names, new Comparator<String>() {
        public int compare(String o1, String o2) {
          return o1.substring(o1.indexOf(".old") - 14).compareTo(o2.substring(o2.indexOf(".old") - 14));
        }
      });
      int todDel = names.length - ngen;
      if (todDel > 0) {
        for (int i = 0; i < todDel; i++) {
          new File(dir, names[i]).delete();
        }
      }
    }
  }

  private void checkRolling() throws IOException {
    long max = rolloverSizeProvider.getRollOverSize();
    if (max == -1)
      return;
    if (length > max) {
      writer.flush();
      writer.close();
      File f = new File(filename + "-" + (generation++) + FMTROTATE.format(new Date()));
      file.renameTo(f);
      file = new File(filename);
      writer = new FileWriter(filename, true);
      length = 0;
      if (numberGenerationProvider != null)
        checkGenerations();
    }
  }

  public synchronized void write(char[] cbuf, int off, int len) throws IOException {
    if (writer == null)
      return;
    writer.write(cbuf, off, len);
    writer.flush();
    length += (len - off);
    checkRolling();
  }

  public synchronized void write(String str) throws IOException {
    if (writer == null)
      return;
    writer.write(str);
    writer.flush();
    length += str.length();
    checkRolling();
  }

  public synchronized void write(String str, int off, int len) throws IOException {
    if (writer == null)
      return;
    writer.write(str, off, len);
    writer.flush();
    length += (len - off);
    checkRolling();
  }

  public synchronized void write(char[] cbuf) throws IOException {
    if (writer == null)
      return;
    writer.write(cbuf);
    writer.flush();
    length += cbuf.length;
    checkRolling();
  }

  public synchronized void flush() throws IOException {
    if (writer == null)
      return;
    writer.flush();
    checkRolling();
  }

  public synchronized void close() throws IOException {
    if (writer == null)
      return;
    writer.flush();
    writer.close();
  }

  public static void main(String[] args) {
    System.setOut(new PrintStream(new OutputStream() {
      byte[] buffer = new byte[1];
      Writer writer = null;

      public void write(byte[] bytes) throws IOException {
        checkWriter();
        writer.write(new String(bytes));
      }

      public void write(byte[] bytes, int i, int i1) throws IOException {
        checkWriter();
        writer.write(new String(bytes, i, i1));
      }

      public void flush() throws IOException {
        checkWriter();
        writer.flush();
      }

      public void close() throws IOException {
        checkWriter();
        writer.close();
      }

      public void write(int i) throws IOException {
        checkWriter();
        buffer[0] = (byte) i;
        writer.write(new String(buffer));
      }

      private void checkWriter() throws IOException {
        if (writer == null) {
          writer = new RollingFileWriter("/Users/am/temp/test.log", new RolloverSizeProvider() {
            public long getRollOverSize() {
              return 100;
            }
          }, new NumberGenerationProvider() {
            public int getNumberGenerations() {
              return 5;
            }
          });
        }
      }
    }));
    for (int i = 0; i < 100; i++) {
      try {
        Thread.currentThread().sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("Dies ist Zeile " + i);
    }
  }
}