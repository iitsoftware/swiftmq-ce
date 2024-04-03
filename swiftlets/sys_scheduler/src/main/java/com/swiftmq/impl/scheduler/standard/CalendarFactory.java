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

import com.swiftmq.impl.scheduler.standard.po.CalendarChanged;
import com.swiftmq.mgmt.*;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class CalendarFactory {
    static final DecimalFormat fmt = new DecimalFormat("00");
    static final SimpleDateFormat drFmt = new SimpleDateFormat("yyyy-MM-dd");

    private static void addChangeListener(SwiftletContext ctx, Entity entity, SchedulerCalendar calendar) {
        Property prop = entity.getProperty("type");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.setExclude(newValue.equals("exclude"));
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("base-calendar");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.setBaseCalendarName((String) newValue);
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("enable-weekdays");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.setEnableWeekDays((Boolean) newValue);
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("enable-monthdays");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.setEnableMonthDays((Boolean) newValue);
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("enable-monthdays-last");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.setEnableMonthDayLast((Boolean) newValue);
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("enable-annualdays");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.setEnableAnnualDays((Boolean) newValue);
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("enable-dateranges");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.setEnableDateRanges((Boolean) newValue);
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        Entity wdEntity = entity.getEntity("weekdays");
        for (int i = 0; i < 7; i++) {
            prop = wdEntity.getProperty("day-" + fmt.format(i + 1));
            prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
                public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                    SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                    SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                    try {
                        int day = fmt.parse(property.getName().substring(4)).intValue();
                        myCal.setWeekDay(day, (Boolean) newValue);
                        myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                    } catch (Exception e) {
                        throw new PropertyChangeException(e.toString());
                    }
                }
            });
        }
        Entity mdEntity = entity.getEntity("monthdays");
        for (int i = 0; i < 31; i++) {
            prop = mdEntity.getProperty("day-" + fmt.format(i + 1));
            prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
                public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                    SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                    SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                    try {
                        int day = fmt.parse(property.getName().substring(4)).intValue();
                        myCal.setMonthDay(day, (Boolean) newValue);
                        myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                    } catch (Exception e) {
                        throw new PropertyChangeException(e.toString());
                    }
                }
            });
        }
        prop = mdEntity.getProperty("last");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, calendar}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                try {
                    myCal.setMonthDay(32, (Boolean) newValue);
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        EntityList adList = (EntityList) entity.getEntity("annualdays");
        adList.setEntityAddListener(new EntityChangeAdapter(new Object[]{ctx, calendar}) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                try {
                    myCal.addAnnualDay(newEntity.getName(), fmt.parse((String) newEntity.getProperty("day").getValue()).intValue(), (String) newEntity.getProperty("month").getValue());
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new EntityAddException(e.toString());
                }
            }
        });
        adList.setEntityRemoveListener(new EntityChangeAdapter(new Object[]{ctx, calendar}) {
            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.removeAnnualDay(delEntity.getName());
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new EntityRemoveException(e.toString());
                }
            }
        });
        EntityList drList = (EntityList) entity.getEntity("date-ranges");
        drList.setEntityAddListener(new EntityChangeAdapter(new Object[]{ctx, calendar}) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                try {
                    checkDate(myCal, newEntity);
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
            }
        });
        drList.setEntityRemoveListener(new EntityChangeAdapter(new Object[]{ctx, calendar}) {
            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                SchedulerCalendar myCal = (SchedulerCalendar) ((Object[]) configObject)[1];
                myCal.removeDateRange(delEntity.getName());
                try {
                    myCtx.scheduler.enqueue(new CalendarChanged(myCal.createCopy()));
                } catch (Exception e) {
                    throw new EntityRemoveException(e.toString());
                }
            }
        });
    }

    public static SchedulerCalendar createCalendar(SwiftletContext ctx, Entity entity) throws Exception {
        SchedulerCalendar cal = null;
        String name = entity.getName();
        String type = (String) entity.getProperty("type").getValue();
        String baseCalendar = (String) entity.getProperty("base-calendar").getValue();
        boolean enableWeekDays = (Boolean) entity.getProperty("enable-weekdays").getValue();
        boolean enableMonthDays = (Boolean) entity.getProperty("enable-monthdays").getValue();
        boolean enableMonthDaysLast = (Boolean) entity.getProperty("enable-monthdays-last").getValue();
        boolean enableAnnualDays = (Boolean) entity.getProperty("enable-annualdays").getValue();
        boolean enableDateRanges = (Boolean) entity.getProperty("enable-dateranges").getValue();
        cal = new SchedulerCalendar(name, type.equals("exclude"), baseCalendar, enableWeekDays, enableMonthDays, enableMonthDaysLast, enableAnnualDays, enableDateRanges);
        Entity wdEntity = entity.getEntity("weekdays");
        for (int i = 0; i < 7; i++) {
            Property prop = wdEntity.getProperty("day-" + fmt.format(i + 1));
            cal.setWeekDay(i + 1, (Boolean) prop.getValue());
        }
        Entity mdEntity = entity.getEntity("monthdays");
        for (int i = 0; i < 31; i++) {
            Property prop = mdEntity.getProperty("day-" + fmt.format(i + 1));
            cal.setMonthDay(i + 1, (Boolean) prop.getValue());
        }
        Property prop = mdEntity.getProperty("last");
        cal.setMonthDay(32, (Boolean) prop.getValue());
        EntityList adList = (EntityList) entity.getEntity("annualdays");
        Map entities = adList.getEntities();
        if (entities != null) {
            for (Object o : entities.entrySet()) {
                Entity e = (Entity) ((Map.Entry<?, ?>) o).getValue();
                cal.addAnnualDay(e.getName(), fmt.parse((String) e.getProperty("day").getValue()).intValue(), (String) e.getProperty("month").getValue());
            }
        }
        EntityList drList = (EntityList) entity.getEntity("date-ranges");
        entities = drList.getEntities();
        if (entities != null) {
            for (Object o : entities.entrySet()) {
                Entity e = (Entity) ((Map.Entry<?, ?>) o).getValue();
                checkDate(cal, e);
            }
        }
        addChangeListener(ctx, entity, cal);
        return cal;
    }

    private static void checkDate(SchedulerCalendar cal, Entity e) throws Exception {
        String from = (String) e.getProperty("from").getValue();
        String to = (String) e.getProperty("to").getValue();
        Date fd = drFmt.parse(from);
        Date td = drFmt.parse(to);
        if (td.getTime() < fd.getTime())
            throw new Exception("Date Range, 'from' (" + from + ") is greater than 'to' (" + to + ")");
        cal.addDateRange(e.getName(), from, to);
    }
}
