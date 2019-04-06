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

package com.swiftmq.impl.net.netty;

import com.swiftmq.tools.prop.SystemProperties;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.List;

public class SSLContextFactory {
    private static final String PROP_KEYSTORE = "javax.net.ssl.keyStore";
    private static final String PROP_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String PROP_TRUSTSTORE = "javax.net.ssl.trustStore";
    private static final String PROP_TRUSTSTORE_PASSWORD = "javax.net.ssl.truestStorePassword";
    private static final String PROP_CERTCHAIN_FILE = "swiftmq.jsse.cert.file";
    private static final String PROP_PRIVATEKEY_FILE = "swiftmq.jsse.privatekey.file";
    private static final String PROP_CLIENT_AUTH_ENABLED = "swiftmq.jsse.clientauth.enabled";

    private static final String[] CIPHERS = new String[] {
            "TLS_DH_anon_WITH_AES_128_CBC_SHA256",
            "TLS_ECDH_anon_WITH_AES_128_CBC_SHA",
            "TLS_DH_anon_WITH_AES_128_CBC_SHA",
            "TLS_ECDH_anon_WITH_RC4_128_SHA",
            "TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA",
            "TLS_ECDH_anon_WITH_NULL_SHA"
    };

    public static SslContext createSwiftMQStandardContext() throws Exception {
        String keyStoreFilename = System.getProperty(PROP_KEYSTORE);
        String keyStorePassword = System.getProperty(PROP_KEYSTORE_PASSWORD);
        String trustStoreFilename = System.getProperty(PROP_TRUSTSTORE);
        String trustStorePassword = System.getProperty(PROP_TRUSTSTORE_PASSWORD);
        KeyManagerFactory kmf = null;
        SslContextBuilder builder = null;
        if (keyStoreFilename != null && keyStorePassword != null) {
            File f = new File(keyStoreFilename);
            if (f.exists()) {
                KeyStore ksKeys = KeyStore.getInstance("JKS");
                ksKeys.load(new FileInputStream(f), keyStorePassword.toCharArray());
                kmf = KeyManagerFactory.getInstance("SunX509");
                kmf.init(ksKeys, keyStorePassword.toCharArray());
                builder = SslContextBuilder.forServer(kmf);
            }
        }
        TrustManagerFactory tmf = null;
        if (trustStoreFilename != null && trustStorePassword != null) {
            File f = new File(trustStoreFilename);
            if (f.exists()) {
                KeyStore ksKeys = KeyStore.getInstance("JKS");
                ksKeys.load(new FileInputStream(f), trustStorePassword.toCharArray());
                tmf = TrustManagerFactory.getInstance("SunX509");
                tmf.init(ksKeys);
                if (builder != null)
                    builder.trustManager(tmf);
            }
        }
        if (builder == null) {
            String certchainFile = System.getProperty(PROP_CERTCHAIN_FILE);
            String privatekeyFile = System.getProperty(PROP_PRIVATEKEY_FILE);
            if (certchainFile == null || privatekeyFile == null)
                throw new Exception("Can't create SslContext! Neither a keystore nor a cert/private key has been specified as system property!");
            File certchain = new File(certchainFile);
            if (!certchain.exists())
                throw new Exception("Can't create SslContext! Certificate file "+certchainFile+" does not exists!");
            File privatekey = new File(privatekeyFile);
            if (!privatekey.exists())
                throw new Exception("Can't create SslContext! Private key file "+privatekeyFile+" does not exists!");
            builder = SslContextBuilder.forServer(certchain, privatekey);
        }
        boolean clientAuth = Boolean.valueOf(SystemProperties.get(PROP_CLIENT_AUTH_ENABLED, "false"));
        if (clientAuth)
            builder.clientAuth(ClientAuth.REQUIRE);

        return builder.build();
    }
}
