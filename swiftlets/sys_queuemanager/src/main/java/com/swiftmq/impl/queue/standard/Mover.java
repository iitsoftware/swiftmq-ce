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

package com.swiftmq.impl.queue.standard;

public class Mover extends Copier
{
  public Mover(SwiftletContext ctx)
  {
    super(ctx);
    setRemove(true);
  }

  protected String _getCommand()
  {
    return "move";
  }

  protected String _getPattern()
  {
    return "move <source> -queue|-topic <target> [(-selector <selector>)|(-index <start> <stop>)] [-maxlimit <nmsgs>]";
  }

  protected String _getDescription()
  {
    return "Move messages from queue <source> to\n" +
        "queue/topic <target>. A subset can be defined\n" +
        "with -selector <selector> or by specifying\n" +
        "a range with -index <start> <stop>,\n" +
        "with a -maxlimit of <nmsgs> to move.";
  }
}
