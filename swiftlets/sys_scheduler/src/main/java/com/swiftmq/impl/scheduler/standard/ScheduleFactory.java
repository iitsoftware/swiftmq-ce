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

import com.swiftmq.impl.scheduler.standard.po.ScheduleChanged;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.scheduler.InvalidScheduleException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ScheduleFactory {
    public static final String NOW = "now";
    public static final String FOREVER = "forever";
    public static final String AT = "at";
    public static final String START = "start";
    public static final String STOP = "stop";
    public static final String DELAY = "delay";
    public static final String REPEAT = "repeat";
    static final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
    static final SimpleDateFormat timeFmt1 = new SimpleDateFormat("HH:mm:ss");
    static final SimpleDateFormat timeFmt2 = new SimpleDateFormat("HH:mm");

    private static long parseTime(String s) throws Exception {
        char unit = s.charAt(s.length() - 1);
        String value = s.substring(0, s.length() - 1);
        int iVal = Integer.parseInt(value);
        long ret = 0;
        switch (unit) {
            case 'h':
                ret = iVal * 60 * 60 * 1000;
                break;
            case 'm':
                ret = iVal * 60 * 1000;
                break;
            case 's':
                ret = iVal * 1000;
                break;
            default:
                throw new Exception("Invalid time unit, please use 'h', 'm', or 's");
        }
        return ret;
    }

    private static boolean isAtExpression(String s) {
        return s.startsWith(AT);
    }

    private static int[] parseAt(String expr) throws Exception {
        //'at' HH:mm[:ss][, HH:mm[:ss]...]
        StringTokenizer t = new StringTokenizer(expr, " ,");
        String token = t.nextToken();
        if (!token.equals(AT))
            throw new Exception("Invalid time expression, '" + AT + "' expected!");
        List list = new ArrayList();
        while (t.hasMoreTokens()) {
            token = t.nextToken();
            Date date = null;
            if (token.length() == 5)
                date = timeFmt2.parse(token);
            else if (token.length() == 8)
                date = timeFmt1.parse(token);
            if (date == null)
                throw new Exception("Invalid time format: " + token + ", HH:mm:ss or HH:mm expected!");
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int time = cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
            list.add(new Integer(time));
        }
        if (list.size() == 0)
            throw new Exception("Missing time!");
        Collections.sort(list);
        int[] ret = new int[list.size()];
        for (int i = 0; i < list.size(); i++)
            ret[i] = ((Integer) list.get(i)).intValue();
        return ret;
    }

    private static int[] parseRepeat(String expr) throws Exception {
        // 'start' HH:mm[:ss] 'stop' HH:mm[:ss] 'delay' n('s'|'m'|'h' ['repeat' n]
        StringTokenizer t = new StringTokenizer(expr, " ,");
        int n = t.countTokens();
        if (n < 6 || n > 8)
            throw new Exception("Invalid time expression, expected format: 'start' HH:mm[:ss] 'stop' HH:mm[:ss] 'delay' n('s'|'m'|'h' ['repeat' n]");
        String token = t.nextToken();
        if (!token.equals(START))
            throw new Exception("Invalid time expression, '" + START + "' expected!");
        token = t.nextToken();
        Date date = null;
        if (token.length() == 5)
            date = timeFmt2.parse(token);
        else if (token.length() == 8)
            date = timeFmt1.parse(token);
        if (date == null)
            throw new Exception("Invalid time format: " + token + ", HH:mm:ss or HH:mm expected!");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int startTime = cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
        token = t.nextToken();
        if (!token.equals(STOP))
            throw new Exception("Invalid time expression, '" + STOP + "' expected!");
        token = t.nextToken();
        date = null;
        if (token.length() == 5)
            date = timeFmt2.parse(token);
        else if (token.length() == 8)
            date = timeFmt1.parse(token);
        if (date == null)
            throw new Exception("Invalid time format: " + token + ", HH:mm:ss or HH:mm expected!");
        cal = Calendar.getInstance();
        cal.setTime(date);
        int stopTime = cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
        token = t.nextToken();
        if (!token.equals(DELAY))
            throw new Exception("Invalid time expression, '" + DELAY + "' expected!");
        int delay = (int) parseTime(t.nextToken()) / 1000;
        int repeat = -1;
        if (n == 8) {
            token = t.nextToken();
            if (!token.equals(REPEAT))
                throw new Exception("Invalid time expression, '" + REPEAT + "' expected!");
            repeat = Integer.parseInt(t.nextToken());
        }
        return new int[]{startTime, stopTime, delay, repeat};
    }

    private static void addChangeListener(SwiftletContext ctx, Entity entity, Schedule schedule) {
        Property prop = entity.getProperty("enabled");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, schedule}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                mySchedule.setEnabled(((Boolean) newValue).booleanValue());
                try {
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("logging-enabled");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, schedule}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                mySchedule.setLoggingEnabled(((Boolean) newValue).booleanValue());
                try {
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("calendar");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, schedule}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                mySchedule.setCalendar((String) newValue);
                try {
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("date-from");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, schedule}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                String s = (String) newValue;
                try {
                    if (s.toLowerCase().equals(NOW))
                        s = null;
                    else
                        fmt.parse(s);
                    if (s != null && mySchedule.getDateTo() != null && s.compareTo(mySchedule.getDateTo()) > 0)
                        throw new Exception("Date From (" + s + ") is greater than Date To (" + mySchedule.getDateTo() + ")!");
                    mySchedule.setDateFrom(s);
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("date-to");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, schedule}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                String s = (String) newValue;
                try {
                    if (s.toLowerCase().equals(FOREVER))
                        s = null;
                    else
                        fmt.parse(s);
                    if (s != null && mySchedule.getDateFrom() != null && s.compareTo(mySchedule.getDateFrom()) < 0)
                        throw new Exception("Date To (" + s + ") is less than Date From (" + mySchedule.getDateFrom() + ")!");
                    mySchedule.setDateTo(s);
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("max-runtime");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, schedule}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                try {
                    String maxRt = (String) newValue;
                    long maxRuntime = -1;
                    if (maxRt != null && maxRt.trim().length() > 0)
                        maxRuntime = parseTime(maxRt);
                    mySchedule.setMaxRuntime(maxRuntime);
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        prop = entity.getProperty("time-expression");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(new Object[]{ctx, schedule}) {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                String old = (String) oldValue;
                String s = (String) newValue;
                if (isAtExpression(old) != isAtExpression(s))
                    throw new PropertyChangeException("You cannot change a 'at' expression to a 'repeat' expression and vice versa! Please re-create the Schedule instead.");
                try {
                    if (isAtExpression(s))
                        ((AtSchedule) mySchedule).setStartTimes(parseAt(s));
                    else {
                        int[] vals = parseRepeat(s);
                        ((RepeatSchedule) mySchedule).setStartTime(vals[0]);
                        ((RepeatSchedule) mySchedule).setEndTime(vals[1]);
                        ((RepeatSchedule) mySchedule).setDelay(vals[2]);
                        ((RepeatSchedule) mySchedule).setRepeats(vals[3]);
                    }
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new PropertyChangeException(e.toString());
                }
            }
        });
        EntityList parmList = (EntityList) entity.getEntity("parameters");
        parmList.setEntityAddListener(new EntityChangeAdapter(new Object[]{ctx, schedule}) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                Map parms = mySchedule.getParameters();
                parms.put(newEntity.getName(), newEntity.getProperty("value").getValue());
                try {
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
            }
        });
        parmList.setEntityRemoveListener(new EntityChangeAdapter(new Object[]{ctx, schedule}) {
            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                SwiftletContext myCtx = (SwiftletContext) ((Object[]) configObject)[0];
                Schedule mySchedule = (Schedule) ((Object[]) configObject)[1];
                Map parms = mySchedule.getParameters();
                parms.remove(delEntity.getName());
                try {
                    myCtx.scheduler.enqueue(new ScheduleChanged(mySchedule.createCopy()));
                } catch (Exception e) {
                    throw new EntityRemoveException(e.toString());
                }
            }
        });
    }

    public static Schedule createSchedule(SwiftletContext ctx, Entity entity) throws Exception {
        Schedule schedule = null;
        String name = entity.getName();
        boolean enabled = ((Boolean) entity.getProperty("enabled").getValue()).booleanValue();
        boolean enabledLogging = ((Boolean) entity.getProperty("logging-enabled").getValue()).booleanValue();
        String calendar = (String) entity.getProperty("calendar").getValue();
        String jobGroup = (String) entity.getProperty("job-group").getValue();
        String jobName = (String) entity.getProperty("job-name").getValue();
        String dateFrom = (String) entity.getProperty("date-from").getValue();
        if (dateFrom.toLowerCase().equals(NOW))
            dateFrom = null;
        else
            fmt.parse(dateFrom);
        String dateTo = (String) entity.getProperty("date-to").getValue();
        if (dateTo.toLowerCase().equals(FOREVER))
            dateTo = null;
        else
            fmt.parse(dateTo);
        if (dateFrom != null && dateTo != null && dateFrom.compareTo(dateTo) > 0)
            throw new Exception("Date From (" + dateFrom + ") is greater than Date To (" + dateTo + ")!");
        String maxRt = (String) entity.getProperty("max-runtime").getValue();
        long maxRuntime = -1;
        if (maxRt != null && maxRt.trim().length() > 0)
            maxRuntime = parseTime(maxRt);
        String timeExpr = ((String) entity.getProperty("time-expression").getValue()).trim().toLowerCase();
        boolean isAt = isAtExpression(timeExpr);
        if (isAt) {
            schedule = new AtSchedule(name, enabled, enabledLogging, jobGroup, jobName, calendar, dateFrom, dateTo, maxRuntime, timeExpr, parseAt(timeExpr));
        } else {
            int[] vals = parseRepeat(timeExpr);
            schedule = new RepeatSchedule(name, enabled, enabledLogging, jobGroup, jobName, calendar, dateFrom, dateTo, maxRuntime, timeExpr, vals[0], vals[1], vals[2], vals[3]);
        }
        EntityList parmList = (EntityList) entity.getEntity("parameters");
        Map entities = parmList.getEntities();
        if (entities != null) {
            Map parameters = schedule.getParameters();
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                parameters.put(e.getName(), e.getProperty("value").getValue());
            }
        }
        addChangeListener(ctx, entity, schedule);
        return schedule;
    }

    public static Schedule createSchedule(SwiftletContext ctx, MessageImpl message, String name, String jobGroup, String jobName, String parm, String value) throws Exception {
        Schedule schedule = null;
        boolean enabledLogging = message.getBooleanProperty(SwiftletContext.PROP_SCHED_ENABLE_LOGGING);
        String calendar = message.getStringProperty(SwiftletContext.PROP_SCHED_CALENDAR);
        String dateFrom = message.getStringProperty(SwiftletContext.PROP_SCHED_FROM);
        if (dateFrom == null)
            throw new Exception("Missing Property: " + SwiftletContext.PROP_SCHED_FROM);
        String dateTo = message.getStringProperty(SwiftletContext.PROP_SCHED_TO);
        if (dateTo == null)
            throw new Exception("Missing Property: " + SwiftletContext.PROP_SCHED_TO);
        String timeExpr = message.getStringProperty(SwiftletContext.PROP_SCHED_TIME);
        if (timeExpr == null)
            throw new Exception("Missing Property: " + SwiftletContext.PROP_SCHED_TIME);
        if (dateFrom.toLowerCase().equals(NOW))
            dateFrom = null;
        else
            fmt.parse(dateFrom);
        if (dateTo.toLowerCase().equals(FOREVER))
            dateTo = null;
        else
            fmt.parse(dateTo);
        if (dateFrom != null && dateTo != null && dateFrom.compareTo(dateTo) > 0)
            throw new Exception("Date From (" + dateFrom + ") is greater than Date To (" + dateTo + ")!");
        boolean isAt = isAtExpression(timeExpr);
        if (isAt) {
            AtSchedule atSchedule = new AtSchedule(name, true, enabledLogging, jobGroup, jobName, calendar, dateFrom, dateTo, -1, timeExpr, parseAt(timeExpr));
            if (message.propertyExists(SwiftletContext.PROP_SCHED_MAY_EXPIRE) && !message.getBooleanProperty(SwiftletContext.PROP_SCHED_MAY_EXPIRE))
                atSchedule.setMayExpireWhileRouterDown(false);
            schedule = atSchedule;
        } else {
            int[] vals = parseRepeat(timeExpr);
            schedule = new RepeatSchedule(name, true, enabledLogging, jobGroup, jobName, calendar, dateFrom, dateTo, -1, timeExpr, vals[0], vals[1], vals[2], vals[3]);
        }
        Map parms = schedule.getParameters();
        parms.put(parm, value);
        return schedule;
    }

    public static Schedule createSchedule(SwiftletContext ctx, String name, String jobGroup, String jobName, String calendar, String dateFrom, String dateTo, String timeExpr, String maxRt, boolean enabledLogging) throws InvalidScheduleException {
        Schedule schedule = null;
        if (timeExpr == null)
            throw new InvalidScheduleException("Missing time expression!");
        if (dateFrom != null && dateFrom.toLowerCase().equals(NOW))
            dateFrom = null;
        if (dateFrom != null) {
            try {
                fmt.parse(dateFrom);
            } catch (ParseException e) {
                throw new InvalidScheduleException(e.getMessage());
            }
        }
        if (dateTo != null && dateTo.toLowerCase().equals(FOREVER))
            dateTo = null;
        if (dateTo != null) {
            try {
                fmt.parse(dateTo);
            } catch (ParseException e) {
                throw new InvalidScheduleException(e.getMessage());
            }
        }
        if (dateFrom != null && dateTo != null && dateFrom.compareTo(dateTo) > 0)
            throw new InvalidScheduleException("Date From (" + dateFrom + ") is greater than Date To (" + dateTo + ")!");
        long maxRuntime = -1;
        try {
            if (maxRt != null && maxRt.trim().length() > 0)
                maxRuntime = parseTime(maxRt);
        } catch (Exception e) {
            throw new InvalidScheduleException(e.getMessage());
        }
        boolean isAt = isAtExpression(timeExpr);
        if (isAt) {
            try {
                schedule = new AtSchedule(name, true, enabledLogging, jobGroup, jobName, calendar, dateFrom, dateTo, maxRuntime, timeExpr, parseAt(timeExpr));
            } catch (Exception e) {
                throw new InvalidScheduleException(e.getMessage());
            }
        } else {
            int[] vals = new int[0];
            try {
                vals = parseRepeat(timeExpr);
            } catch (Exception e) {
                throw new InvalidScheduleException(e.getMessage());
            }
            schedule = new RepeatSchedule(name, true, enabledLogging, jobGroup, jobName, calendar, dateFrom, dateTo, maxRuntime, timeExpr, vals[0], vals[1], vals[2], vals[3]);
        }
        return schedule;
    }
}
