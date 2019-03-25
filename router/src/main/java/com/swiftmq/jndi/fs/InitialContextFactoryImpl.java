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

package com.swiftmq.jndi.fs;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;

public class InitialContextFactoryImpl implements InitialContextFactory, java.io.Serializable {
    public Context getInitialContext(Hashtable env) throws NamingException {
        if (env == null)
            throw new NamingException("Environment is null");
        String dirName = (String) env.get(Context.PROVIDER_URL);
        if (dirName == null)
            throw new NamingException("Missing environment property " + Context.PROVIDER_URL);
        try {
            File dir = new File(new URI(dirName));
            if (!dir.exists())
                throw new NamingException("Directory does not exist: " + dirName);
            return new ContextImpl(dir);
        } catch (URISyntaxException e) {
            throw new NamingException(e.getMessage());
        }
    }
}
