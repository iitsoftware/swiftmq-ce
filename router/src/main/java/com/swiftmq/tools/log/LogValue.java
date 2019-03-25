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

import java.io.Serializable;
import java.util.Date;

/**
 * LogValue beinhaltet die zu loggenden Informationen, bestehend aus Source, Level und der
 * entsprechenden Message.
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class LogValue implements Serializable {
    public final static int INFORMATION = 0;
    public final static int WARNING = -1;
    public final static int ERROR = -2;
    String source = null;
    long time = 0;
    int level = 0;
    String message = null;

    /**
     * Erzeugt ein neues LogValu
     *
     * @param source
     * @param level
     * @param message
     * @SBGen Constructor assigns source, level, message
     */
    public LogValue(String source, int level, String message) {
        // SBgen: Assign variables
        this.source = source;
        this.level = level;
        this.message = message;
        // SBgen: End assign
        time = System.currentTimeMillis();
        //{{INIT_CONTROLS
        //}}
    }

    /**
     * Rueckgabe der Source
     *
     * @SBGen Method get source
     */
    public String getSource() {
        // SBgen: Get variable
        return (source);
    }

    /**
     * Rueckgabe des LogLevels
     *
     * @SBGen Method get level
     */
    public int getLevel() {
        // SBgen: Get variable
        return (level);
    }

    /**
     * Rueckgabe der Messag
     *
     * @SBGen Method get message
     */
    public String getMessage() {
        // SBgen: Get variable
        return (message);
    }

    /**
     * Rueckgabe der LogTime
     *
     * @SBGen Method get time
     */
    public long getTime() {
        // SBgen: Get variable
        return (time);
    }

    /**
     * Erzeugt einen lesbaren LogOutput
     */
    public String toString() {
        String sLevel = null;
        switch (level) {
            case INFORMATION:
                sLevel = "INFORMATION";
                break;
            case WARNING:
                sLevel = "WARNING";
                break;
            case ERROR:
                sLevel = "ERROR";
                break;
        }
        return "[" + new Date(time) + "] " + source + "/" + sLevel + "/" + message;
    }
    //{{DECLARE_CONTROLS
    //}}
}

