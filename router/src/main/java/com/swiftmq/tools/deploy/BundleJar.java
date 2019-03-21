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


public class BundleJar
{
  String filename = null;
  long lastModified = -1;

  BundleJar(String filename, long lastModified)
  {
    this.filename = filename;
    this.lastModified = lastModified;
  }

  String getFilename()
  {
    return filename;
  }

  void setLastModified(long lastModified)
  {
    this.lastModified = lastModified;
  }

  long getLastModified()
  {
    return lastModified;
  }

  public String toString()
  {
    return "[BundleJar, filename=" + filename + ", lastModified=" + lastModified + "]";
  }
}

