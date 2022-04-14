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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.starlight.grpc.proto.PulsarGrpc;
import com.datastax.oss.starlight.grpc.utils.PulsarCluster;
import com.google.common.collect.Sets;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TlsConnectionTest {

  private static final String TLS_TRUST_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/cacert.pem";
  private static final String TLS_GATEWAY_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/broker-cert.pem";
  private static final String TLS_GATEWAY_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/broker-key.pem";
  private static final String TLS_CLIENT_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/client-cert.pem";
  private static final String TLS_CLIENT_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/client-key.pem";

  private static final String BROKER_KEYSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/broker.keystore.jks";
  private static final String BROKER_TRUSTSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/broker.truststore.jks";
  private static final String BROKER_KEYSTORE_PW = "111111";
  private static final String BROKER_TRUSTSTORE_PW = "111111";

  private static final String CLIENT_KEYSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/client.keystore.jks";
  private static final String CLIENT_TRUSTSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/client.truststore.jks";
  private static final String CLIENT_KEYSTORE_PW = "111111";
  private static final String CLIENT_TRUSTSTORE_PW = "111111";

  private static final String KEYSTORE_TYPE = "JKS";
  private static final StringValue PING = StringValue.of("ping");

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  private GatewayService gatewayService;
  private GatewayConfiguration config;
  private ManagedChannel channel;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @BeforeEach
  public void beforeEach() {
    CollectorRegistry.defaultRegistry.clear();
    int port = PortManager.nextFreePort();

    config = new GatewayConfiguration();
    config.setGrpcServicePortTls(port);
    config.setBrokerServiceURL(cluster.getAddress());
    config.setBrokerWebServiceURL(cluster.getAddress());
    config.setConfigurationStoreServers(
        cluster.getService().getConfig().getConfigurationStoreServers());

    config.setTlsKeyStoreType(KEYSTORE_TYPE);
    config.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
    config.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
    config.setTlsTrustStoreType(KEYSTORE_TYPE);
    config.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
    config.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);

    config.setTlsCertificateFilePath(TLS_GATEWAY_CERT_FILE_PATH);
    config.setTlsKeyFilePath(TLS_GATEWAY_KEY_FILE_PATH);
    config.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);

    config.setTlsRequireTrustedClientCertOnConnect(true);

    config.setAuthenticationProviders(
        Sets.newHashSet("org.apache.pulsar.broker.authentication.AuthenticationProviderTls"));
  }

  private void startGatewayService() throws IOException, GeneralSecurityException {
    PulsarService pulsar = cluster.getService();
    ClusterDataImpl clusterData =
        ClusterDataImpl.builder()
            .serviceUrl(pulsar.getWebServiceAddress())
            .serviceUrlTls(pulsar.getWebServiceAddressTls())
            .brokerServiceUrl(pulsar.getBrokerServiceUrl())
            .brokerServiceUrlTls(pulsar.getBrokerServiceUrlTls())
            .build();
    gatewayService =
        new GatewayService(
            config,
            new AuthenticationService(ConfigurationUtils.convertFrom(config)),
            pulsar.getBrokerService().getAuthorizationService(),
            clusterData);
    gatewayService.start();
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (channel == null) {
      channel.shutdownNow();
      channel.awaitTermination(30, TimeUnit.SECONDS);
    }
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @Test
  void testTlsConnectionSuccess() throws Exception {
    startGatewayService();

    SslContextBuilder sslCtx =
        SecurityUtility.createNettySslContextForClient(
            null,
            false,
            TLS_TRUST_CERT_FILE_PATH,
            TLS_CLIENT_CERT_FILE_PATH,
            TLS_CLIENT_KEY_FILE_PATH,
            new TreeSet<>(),
            new TreeSet<>());

    channel =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPortTLS().orElse(-1))
            .sslContext(GrpcSslContexts.configure(sslCtx).build())
            .build();
    PulsarGrpc.PulsarBlockingStub stub = PulsarGrpc.newBlockingStub(channel);
    stub.ping(PING);
  }

  @Test
  void testTlsConnectionFailure() throws Exception {
    startGatewayService();

    channel =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPortTLS().orElse(-1))
            .build();
    PulsarGrpc.PulsarBlockingStub stub = PulsarGrpc.newBlockingStub(channel);
    assertThrows(StatusRuntimeException.class, () -> stub.ping(PING));
  }

  @Test
  void testKeyStoreTlsConnectionSuccess() throws Exception {
    config.setTlsEnabledWithKeyStore(true);
    startGatewayService();

    KeyStoreSSLContext sslCtx =
        new KeyStoreSSLContext(
            KeyStoreSSLContext.Mode.CLIENT,
            null,
            KEYSTORE_TYPE,
            CLIENT_KEYSTORE_FILE_PATH,
            CLIENT_KEYSTORE_PW,
            false,
            KEYSTORE_TYPE,
            BROKER_TRUSTSTORE_FILE_PATH,
            BROKER_TRUSTSTORE_PW,
            true,
            null,
            null);

    SslContext sslContext =
        GrpcSslContexts.forClient()
            .trustManager(sslCtx.createTrustManagerFactory())
            .keyManager(sslCtx.createKeyManagerFactory())
            .clientAuth(ClientAuth.REQUIRE)
            .build();

    channel =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPortTLS().orElse(-1))
            .sslContext(sslContext)
            .build();

    PulsarGrpc.PulsarBlockingStub stub = PulsarGrpc.newBlockingStub(channel);
    stub.ping(PING);
  }

  @Test
  void testKeyStoreTlsConnectionFailure() throws Exception {
    config.setTlsEnabledWithKeyStore(true);
    startGatewayService();

    channel =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPortTLS().orElse(-1))
            .build();
    PulsarGrpc.PulsarBlockingStub stub = PulsarGrpc.newBlockingStub(channel);
    assertThrows(StatusRuntimeException.class, () -> stub.ping(PING));
  }

  @Test
  void testTlsAuthenticationSuccess() throws Exception {
    config.setAuthenticationEnabled(true);
    startGatewayService();

    SslContextBuilder sslCtx =
        SecurityUtility.createNettySslContextForClient(
            null,
            false,
            TLS_TRUST_CERT_FILE_PATH,
            TLS_CLIENT_CERT_FILE_PATH,
            TLS_CLIENT_KEY_FILE_PATH,
            new TreeSet<>(),
            new TreeSet<>());

    channel =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPortTLS().orElse(-1))
            .sslContext(GrpcSslContexts.configure(sslCtx).build())
            .build();
    PulsarGrpc.PulsarBlockingStub stub = PulsarGrpc.newBlockingStub(channel);
    stub.ping(PING);
  }

  @Test
  void testTlsAuthenticationFailure() throws Exception {
    config.setTlsRequireTrustedClientCertOnConnect(false);
    config.setAuthenticationEnabled(true);
    startGatewayService();

    SslContextBuilder sslCtx =
        SecurityUtility.createNettySslContextForClient(
            null,
            false,
            TLS_TRUST_CERT_FILE_PATH,
            null,
            (String) null,
            new TreeSet<>(),
            new TreeSet<>());

    channel =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPortTLS().orElse(-1))
            .sslContext(GrpcSslContexts.configure(sslCtx).build())
            .build();
    PulsarGrpc.PulsarBlockingStub stub = PulsarGrpc.newBlockingStub(channel);

    try {
      stub.ping(PING);
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
      assertEquals("Authentication required", e.getStatus().getDescription());
    }
  }
}
