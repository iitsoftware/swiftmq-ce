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
import java.text.DecimalFormat;
import java.util.Arrays;

public class GenerationalFileWriter extends Writer
{
  static DecimalFormat FMTROTATE = new DecimalFormat("'-'000'.log'");
  String directory = null;
  String filename = null;
  File file = null;
  FileWriter writer = null;
  long length = 0;
  int generation = 0;
  RolloverSizeProvider rolloverSizeProvider = null;
  NumberGenerationProvider numberGenerationProvider = null;

  public GenerationalFileWriter(String directory, String filename, RolloverSizeProvider rolloverSizeProvider, NumberGenerationProvider numberGenerationProvider) throws IOException
  {
    this.directory = directory;
    this.filename = filename;
    this.rolloverSizeProvider = rolloverSizeProvider;
    this.numberGenerationProvider = numberGenerationProvider;
    newLogfile();
  }

  private void checkGenerations() throws IOException
  {
    File dir = new File(directory);
    if (!dir.exists())
    {
      dir.mkdir();
      return;
    }
    String[] names = dir.list(new FilenameFilter()
    {
      public boolean accept(File file, String name)
      {
        return name.startsWith(filename) && name.endsWith(".log");
      }
    });
    if (names != null)
    {
      Arrays.sort(names);
      for (int i = 0; i < names.length; i++)
      {
        int logIdx = names[i].indexOf(".log");
        if (logIdx - 3 >= 0)
        {
          int g = Integer.parseInt(names[i].substring(logIdx - 3, logIdx));
          generation = Math.max(g, generation);
        }
      }
      int todDel = names.length - numberGenerationProvider.getNumberGenerations();
      if (generation == 999)
      {
        generation = 0;
        todDel = names.length;
      }
      if (todDel > 0)
      {
        for (int i = 0; i < todDel; i++)
        {
          new File(dir, names[i]).delete();
        }
      }
    }
  }

  private void newLogfile() throws IOException
  {
    checkGenerations();
    file = new File(directory + File.separator + filename + FMTROTATE.format(generation++));
    if (file.exists())
      length = file.length();
    else
      length = 0;
    writer = new FileWriter(file, true);
  }

  private void checkRolling() throws IOException
  {
    long max = rolloverSizeProvider.getRollOverSize();
    if (max == -1)
      return;
    if (length > max)
    {
      writer.flush();
      writer.close();
      newLogfile();
    }
  }

  public synchronized void write(char[] cbuf, int off, int len) throws IOException
  {
    if (writer == null)
      return;
    writer.write(cbuf, off, len);
    writer.flush();
    length += (len - off);
    checkRolling();
  }

  public synchronized void write(String str) throws IOException
  {
    if (writer == null)
      return;
    writer.write(str);
    writer.flush();
    length += str.length();
    checkRolling();
  }

  public synchronized void write(String str, int off, int len) throws IOException
  {
    if (writer == null)
      return;
    writer.write(str, off, len);
    writer.flush();
    length += (len - off);
    checkRolling();
  }

  public synchronized void write(char[] cbuf) throws IOException
  {
    if (writer == null)
      return;
    writer.write(cbuf);
    writer.flush();
    length += cbuf.length;
    checkRolling();
  }

  public synchronized void flush() throws IOException
  {
    if (writer == null)
      return;
    writer.flush();
    checkRolling();
  }

  public synchronized void close() throws IOException
  {
    if (writer == null)
      return;
    writer.flush();
    writer.close();
  }

  public static void main(String[] args)
  {
    System.setOut(new PrintStream(new OutputStream()
    {
      byte[] buffer = new byte[1];
      Writer writer = null;

      public void write(byte[] bytes) throws IOException
      {
        checkWriter();
        writer.write(new String(bytes));
      }

      public void write(byte[] bytes, int i, int i1) throws IOException
      {
        checkWriter();
        writer.write(new String(bytes, i, i1));
      }

      public void flush() throws IOException
      {
        checkWriter();
        writer.flush();
      }

      public void close() throws IOException
      {
        checkWriter();
        writer.close();
      }

      public void write(int i) throws IOException
      {
        checkWriter();
        buffer[0] = (byte) i;
        writer.write(new String(buffer));
      }

      private void checkWriter() throws IOException
      {
        if (writer == null)
        {
          writer = new GenerationalFileWriter(".", "test", new RolloverSizeProvider()
          {
            public long getRollOverSize()
            {
              return 100;
            }
          }, new NumberGenerationProvider()
          {
            public int getNumberGenerations()
            {
              return 10;
            }
          }
          );
        }
      }
    }));
    for (int i = 0; i < 100; i++)
    {
      System.out.println("Dies ist Zeile " + i);
    }
  }
}