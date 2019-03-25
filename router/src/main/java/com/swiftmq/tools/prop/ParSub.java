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

package com.swiftmq.tools.prop;

/**
 * A useful class to substitute string parameters
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class ParSub {
    public static final String DEFAULT_PARMFLAG = "$";

    public static String substitute(String source, String parm) {
        return substitute(DEFAULT_PARMFLAG, source, new String[]{parm});
    }

    public static String substitute(String source, String[] parm) {
        return substitute(DEFAULT_PARMFLAG, source, parm);
    }

    public static String substitute(String pflag, String source, String[] parm) {
        String result = new String(source);
        int oldIdx = 0;
        int idx = 0;
        for (int i = 0; i < parm.length; i++) {
            idx = result.indexOf(pflag + i);
            while (idx != -1) {
                StringBuffer s = new StringBuffer(result.substring(0, idx));
                s.append(parm[i]);
                s.append(result.substring(idx + (new String(pflag + i)).length()));
                result = s.toString();
                idx = result.indexOf(pflag + i);
            }
        }
        return result;
    }
}
