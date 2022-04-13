/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.starlight.grpc;

import static org.apache.pulsar.common.util.SecurityUtility.getProvider;

import com.google.common.base.Strings;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * KeyStoreSSLContext that mainly wrap a SSLContext to provide SSL context for both webservice and
 * netty.
 */
@Slf4j
public class KeyStoreSSLContext {
  public static final String DEFAULT_KEYSTORE_TYPE = "JKS";
  public static final String DEFAULT_SSL_PROTOCOL = "TLS";
  public static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1.3,TLSv1.2";
  public static final String DEFAULT_SSL_KEYMANGER_ALGORITHM =
      KeyManagerFactory.getDefaultAlgorithm();
  public static final String DEFAULT_SSL_TRUSTMANAGER_ALGORITHM =
      TrustManagerFactory.getDefaultAlgorithm();

  public static final Provider BC_PROVIDER = getProvider();

  /** Connection Mode for TLS. */
  public enum Mode {
    CLIENT,
    SERVER
  }

  @Getter private final Mode mode;

  private final String sslProviderString;
  private final String keyStoreTypeString;
  private final String keyStorePath;
  private final String keyStorePassword;
  private final boolean allowInsecureConnection;
  private final String trustStoreTypeString;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final boolean needClientAuth;
  private final Set<String> ciphers;
  private final Set<String> protocols;
  private SSLContext sslContext;

  private final String protocol = DEFAULT_SSL_PROTOCOL;
  private final String kmfAlgorithm = DEFAULT_SSL_KEYMANGER_ALGORITHM;
  private final String tmfAlgorithm = DEFAULT_SSL_TRUSTMANAGER_ALGORITHM;

  // only init vars, before using it, need to call createSSLContext to create ssl context.
  public KeyStoreSSLContext(
      Mode mode,
      String sslProviderString,
      String keyStoreTypeString,
      String keyStorePath,
      String keyStorePassword,
      boolean allowInsecureConnection,
      String trustStoreTypeString,
      String trustStorePath,
      String trustStorePassword,
      boolean requireTrustedClientCertOnConnect,
      Set<String> ciphers,
      Set<String> protocols) {
    this.mode = mode;
    this.sslProviderString = sslProviderString;
    this.keyStoreTypeString =
        Strings.isNullOrEmpty(keyStoreTypeString) ? DEFAULT_KEYSTORE_TYPE : keyStoreTypeString;
    this.keyStorePath = keyStorePath;
    this.keyStorePassword = keyStorePassword;
    this.trustStoreTypeString =
        Strings.isNullOrEmpty(trustStoreTypeString) ? DEFAULT_KEYSTORE_TYPE : trustStoreTypeString;
    this.trustStorePath = trustStorePath;
    if (trustStorePassword == null) {
      this.trustStorePassword = "";
    } else {
      this.trustStorePassword = trustStorePassword;
    }
    this.needClientAuth = requireTrustedClientCertOnConnect;

    if (protocols != null && protocols.size() > 0) {
      this.protocols = protocols;
    } else {
      this.protocols =
          new HashSet<>(Arrays.asList(DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")));
    }

    if (ciphers != null && ciphers.size() > 0) {
      this.ciphers = ciphers;
    } else {
      this.ciphers = null;
    }

    this.allowInsecureConnection = allowInsecureConnection;
  }

  public KeyManagerFactory createKeyManagerFactory() throws GeneralSecurityException, IOException {
    if (!Strings.isNullOrEmpty(keyStorePath)) {
      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(kmfAlgorithm);
      KeyStore keyStore = KeyStore.getInstance(keyStoreTypeString);
      char[] passwordChars = keyStorePassword.toCharArray();
      try (FileInputStream inputStream = new FileInputStream(keyStorePath)) {
        keyStore.load(inputStream, passwordChars);
      }
      keyManagerFactory.init(keyStore, passwordChars);
      return keyManagerFactory;
    }
    return null;
  }

  public TrustManagerFactory createTrustManagerFactory()
      throws GeneralSecurityException, IOException {
    if (this.allowInsecureConnection) {
      return InsecureTrustManagerFactory.INSTANCE;
    } else {
      TrustManagerFactory trustManagerFactory =
          sslProviderString != null
              ? TrustManagerFactory.getInstance(tmfAlgorithm, sslProviderString)
              : TrustManagerFactory.getInstance(tmfAlgorithm);
      KeyStore trustStore = KeyStore.getInstance(trustStoreTypeString);
      char[] passwordChars = trustStorePassword.toCharArray();
      try (FileInputStream inputStream = new FileInputStream(trustStorePath)) {
        trustStore.load(inputStream, passwordChars);
      }
      trustManagerFactory.init(trustStore);
      return trustManagerFactory;
    }
  }

  public SslContextBuilder createServerSslContextBuider()
      throws GeneralSecurityException, IOException {
    KeyManagerFactory keyManagerFactory = createKeyManagerFactory();
    if (keyManagerFactory == null) {
      return null;
    }
    return SslContextBuilder.forServer(keyManagerFactory)
        .trustManager(createTrustManagerFactory())
        .clientAuth(needClientAuth ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL)
        .ciphers(ciphers)
        .protocols(protocols);
  }
}
