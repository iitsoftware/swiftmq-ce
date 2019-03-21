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

package com.swiftmq.impl.scheduler.standard;

import java.io.*;

public class Util
{
  static public Object deepCopy(Object oldObj) throws Exception
  {
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    Object obj = null;

    try
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(); // A
      oos = new ObjectOutputStream(bos); // B

      // serialize and pass the object
      oos.writeObject(oldObj); // C
      oos.flush(); // D

      ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray()); // E
      ois = new ObjectInputStream(bin); // F

      // return the new object
      obj = ois.readObject(); // G

    } finally
    {
      try
      {
        oos.close();
        ois.close();
      } catch (Exception e1)
      {
      }
    }
    return obj;
  }


}
