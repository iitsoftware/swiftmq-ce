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

package com.swiftmq.impl.accounting.standard.factoryimpl;

import com.swiftmq.impl.accounting.standard.SwiftletContext;
import com.swiftmq.swiftlet.accounting.*;

import java.util.*;

public class JDBCSinkFactory implements AccountingSinkFactory {
    SwiftletContext ctx = null;
    Map parms = null;

    public JDBCSinkFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        parms = new HashMap();
        Parameter p = new Parameter("JDBC Driver Class Name", "Name of the JDBC Driver Class", null, true, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    Class.forName(value);
                } catch (ClassNotFoundException e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("JDBC URL", "JDBC URL", null, true, null);
        parms.put(p.getName(), p);
        p = new Parameter("Username", "JDBC Username", null, false, null);
        parms.put(p.getName(), p);
        p = new Parameter("Password", "JDBC Password", null, false, null);
        parms.put(p.getName(), p);
        p = new Parameter("Insert Statement", "The SQL Insert Statement", null, true, null);
        parms.put(p.getName(), p);
        p = new Parameter("Field Mapping", "Comma-separated list of accounting field names which must correspond to the 'values' order of the SQL Insert Statement", null, true, null);
        parms.put(p.getName(), p);
        p = new Parameter("Type Mapping", "Comma-separated list of types (int, string, long, double) of the fields in 'Field Mapping'", null, true, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                StringTokenizer t = new StringTokenizer(value, ", ");
                while (t.hasMoreTokens()) {
                    String s = t.nextToken();
                    s = s.toLowerCase();
                    if (!((s.equals("int") || s.equals("string") || s.equals("long") || s.equals("double"))))
                        throw new InvalidValueException("Invalid type: " + s + ". Must be one of: int, string, long, double");

                }
            }
        });
        parms.put(p.getName(), p);
    }

    public boolean isSingleton() {
        return false;
    }

    public String getGroup() {
        return "Accounting";
    }

    public String getName() {
        return "JDBCSinkFactory";
    }

    public Map getParameters() {
        return parms;
    }

    public AccountingSink create(Map map) throws Exception {
        String className = (String) map.get("JDBC Driver Class Name");
        String url = (String) map.get("JDBC URL");
        String username = (String) map.get("Username");
        String password = (String) map.get("Password");
        String statement = (String) map.get("Insert Statement");
        String s = (String) map.get("Field Mapping");
        StringTokenizer t = new StringTokenizer(s, ", ");
        List fieldMapping = new ArrayList();
        while (t.hasMoreTokens())
            fieldMapping.add(t.nextToken());
        s = (String) map.get("Type Mapping");
        t = new StringTokenizer(s, ", ");
        List typeMapping = new ArrayList();
        while (t.hasMoreTokens())
            typeMapping.add(t.nextToken());
        if (typeMapping.size() != fieldMapping.size())
            throw new Exception("Please specify a type mapping for all field in 'Field Mapping'");
        return new JDBCSink(ctx, className, url, username, password, statement, fieldMapping, typeMapping);
    }
}
