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

package com.swiftmq.impl.scheduler.standard.po;

import com.swiftmq.tools.pipeline.POVisitor;

public interface EventVisitor extends POVisitor
{
  public void visit(CalendarAdded po);
  public void visit(CalendarRemoved po);
  public void visit(CalendarChanged po);
  public void visit(ScheduleAdded po);
  public void visit(ScheduleRemoved po);
  public void visit(ScheduleChanged po);
  public void visit(JobFactoryAdded po);
  public void visit(JobFactoryRemoved po);
  public void visit(JobStart po);
  public void visit(JobStop po);
  public void visit(JobTerminated po);
  public void visit(Close po);
}
