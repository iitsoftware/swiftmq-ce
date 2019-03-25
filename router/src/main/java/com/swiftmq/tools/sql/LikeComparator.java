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

package com.swiftmq.tools.sql;


public class LikeComparator {
    public static final char SINGLE = '_';
    public static final char ANY = '%';
    private static final char NOT_SET = 255;

    public static boolean compare(String s, String condition, char escapeChar) {
        return compare(s, condition, escapeChar, SINGLE, ANY);
    }

    private static int getLastPosOfChar(String s, char stopChar, int pos) {
        int startPos = pos;
        if (pos + 1 < s.length()) {
            for (int i = startPos + 1; i < s.length(); i++) {
                if (s.charAt(i) == stopChar)
                    startPos++;
                else
                    break;
            }
        }
        return startPos;
    }

    public static boolean compare(String s, String condition, char escapeChar, char singleChar, char anyChar) {
        if (condition.length() == 1 && condition.charAt(0) == anyChar) // the case '%', matches anything
            return true;
        if (condition.length() == 0 && s.length() > 0) // the case ''
            return false;

        int spos = 0;
        int cpos = 0;
        boolean prevWasEscape = false;
        char cchar = NOT_SET;
        try {
            while (spos < s.length()) {
                cchar = condition.charAt(cpos++);
                if (escapeChar != NOT_SET && cchar == escapeChar)
                    prevWasEscape = true;
                else {
                    if (cchar == singleChar && !prevWasEscape) {
                        if (cpos == condition.length())
                            return spos == s.length() - 1; // the last char in condition, so any does match
                        spos++; // forward one single character
                    } else if (cchar == anyChar && !prevWasEscape) {
                        if (cpos == condition.length())
                            return true; // the last char in condition, so any does match

                        // spool forward until there is a match with the stop char
                        // or the end of the string is reached (not match)
                        char stopChar = condition.charAt(cpos);
                        for (; ; ) {
                            if (s.charAt(spos) == stopChar) {
                                // There may be multiple such stopChars like in 12333 and 12%3 so spool forward to the last one
                                int prevSPos = spos;
                                spos = getLastPosOfChar(s, stopChar, spos);
                                int sCharCnt = spos - prevSPos;
                                int prevCPos = cpos;
                                cpos = getLastPosOfChar(condition, stopChar, cpos);
                                int cCharCnt = cpos - prevCPos;

                                // No match if the conditions stop char cnt > that of the expression like in 12333 and 12%3333
                                if (cCharCnt > sCharCnt)
                                    return false;

                                // look ahead if string behind the anyChar matches
                                int lookSPos = spos;
                                int lookCPos = cpos;
                                boolean ok = true;
                                while (lookSPos < s.length() && lookCPos < condition.length() && ok) {
                                    if (condition.charAt(lookCPos) == anyChar || condition.charAt(lookCPos) == singleChar)
                                        break;
                                    else if (escapeChar != NOT_SET && condition.charAt(lookCPos) == escapeChar) {
                                        lookCPos++;
                                        if (lookCPos == condition.length())
                                            break;
                                    }
                                    if (s.charAt(lookSPos) != condition.charAt(lookCPos)) {
                                        ok = false;
                                        break;
                                    }
                                    lookSPos++;
                                    lookCPos++;
                                }
                                if (lookSPos < s.length() && lookCPos == condition.length())
                                    return false;
                                spos = lookSPos;
                                if (ok) {
                                    cpos = lookCPos;
                                    break;
                                }
                            }
                            spos++;
                            if (spos == s.length())
                                return false;
                        }
                    } else if (cchar != s.charAt(spos++))
                        return false;
                    prevWasEscape = false;
                }
            }
        } catch (Exception e) {
            return false;
        } // workaround for c="sal", s="sales"

        return cpos == condition.length() || cpos == condition.length() - 1 && condition.charAt(condition.length() - 1) == ANY;
    }
}
