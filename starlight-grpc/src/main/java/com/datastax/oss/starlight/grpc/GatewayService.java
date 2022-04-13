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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayService {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayService.class);

  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private PulsarResources pulsarResources;
  private final GatewayConfiguration config;

  PulsarClient pulsarClient;
  private ClusterData localCluster;
  private Server server;
  private Server tlsServer;

  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(
          2 * Runtime.getRuntime().availableProcessors(),
          new DefaultThreadFactory("starlight-for-grpc"));

  public GatewayService(
      GatewayConfiguration config,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      ClusterData localCluster) {
    this.config = config;
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.localCluster = localCluster;
  }

  public GatewayService(
      GatewayConfiguration config,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      PulsarResources pulsarResources) {
    this.config = config;
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.pulsarResources = pulsarResources;
  }

  public void start() throws IOException, GeneralSecurityException {
    PulsarGrpcService pulsarGrpcService = new PulsarGrpcService(this);
    List<ServerInterceptor> interceptors = new ArrayList<>();
    interceptors.add(new GrpcProxyServerInterceptor());
    if (config.isAuthenticationEnabled()) {
      interceptors.add(new AuthenticationInterceptor(authenticationService));
    }

    String bindAddress =
        ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getBindAddress());

    Integer grpcServicePort = config.getGrpcServicePort();
    if (grpcServicePort != null) {
      server =
          NettyServerBuilder.forAddress(new InetSocketAddress(bindAddress, grpcServicePort))
              .addService(ServerInterceptors.intercept(pulsarGrpcService, interceptors))
              // .directExecutor()
              .build()
              .start();
      LOG.info("gRPC Service started, listening on " + server.getPort());
    }

    Integer grpcServicePortTls = config.getGrpcServicePortTls();
    if (grpcServicePortTls != null) {
      SslContextBuilder sslContext;
      if (config.isTlsEnabledWithKeyStore()) {
        KeyStoreSSLContext keyStoreSSLContext =
            new KeyStoreSSLContext(
                KeyStoreSSLContext.Mode.SERVER,
                config.getTlsProvider(),
                config.getTlsKeyStoreType(),
                config.getTlsKeyStore(),
                config.getTlsKeyStorePassword(),
                config.isTlsAllowInsecureConnection(),
                config.getTlsTrustStoreType(),
                config.getTlsTrustStore(),
                config.getTlsTrustStorePassword(),
                config.isTlsRequireTrustedClientCertOnConnect(),
                config.getTlsCiphers(),
                config.getTlsProtocols());

        sslContext = keyStoreSSLContext.createServerSslContextBuider();
        if (sslContext == null) {
          LOG.error("Couldn't start gRPC TLS service due to missing keystore param");
          return;
        }
      } else {
        sslContext =
            SecurityUtility.createNettySslContextForServer(
                null,
                config.isTlsAllowInsecureConnection(),
                config.getTlsTrustCertsFilePath(),
                config.getTlsCertificateFilePath(),
                config.getTlsKeyFilePath(),
                config.getTlsCiphers(),
                config.getTlsProtocols(),
                config.isTlsRequireTrustedClientCertOnConnect());
      }
      tlsServer =
          NettyServerBuilder.forAddress(new InetSocketAddress(bindAddress, grpcServicePortTls))
              .addService(ServerInterceptors.intercept(pulsarGrpcService, interceptors))
              .sslContext(GrpcSslContexts.configure(sslContext).build())
              // .directExecutor()
              .build()
              .start();
      LOG.info("gRPC TLS Service started, listening on " + tlsServer.getPort());
    }
  }

  public synchronized PulsarClient getPulsarClient() throws IOException {
    // Do lazy initialization of client
    if (pulsarClient == null) {
      if (localCluster == null) {
        // If not explicitly set, read clusters data from ZK
        localCluster = retrieveClusterData();
      }
      pulsarClient = createClientInstance(localCluster);
    }
    return pulsarClient;
  }

  private PulsarClient createClientInstance(ClusterData clusterData) throws IOException {
    ClientBuilder clientBuilder =
        PulsarClient.builder() //
            .statsInterval(0, TimeUnit.SECONDS) //
            .allowTlsInsecureConnection(config.isTlsAllowInsecureConnection()) //
            .tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath()) //
            .ioThreads(config.getGrpcSocketNumIoThreads()) //
            .connectionsPerBroker(config.getGrpcSocketConnectionsPerBroker());

    if (isNotBlank(config.getBrokerClientAuthenticationPlugin())
        && isNotBlank(config.getBrokerClientAuthenticationParameters())) {
      if (isNotBlank(config.getGrpcBrokerClientAuthenticationParameters())) {
        clientBuilder.authentication(
            config.getBrokerClientAuthenticationPlugin(),
            config.getGrpcBrokerClientAuthenticationParameters());
      } else {
        clientBuilder.authentication(
            config.getBrokerClientAuthenticationPlugin(),
            config.getBrokerClientAuthenticationParameters());
      }
    }

    if (config.isTlsEnabledWithBroker()) {
      if (isNotBlank(clusterData.getBrokerServiceUrlTls())) {
        clientBuilder.serviceUrl(clusterData.getBrokerServiceUrlTls());
      } else if (isNotBlank(clusterData.getServiceUrlTls())) {
        clientBuilder.serviceUrl(clusterData.getServiceUrlTls());
      }
    } else if (isNotBlank(clusterData.getBrokerServiceUrl())) {
      clientBuilder.serviceUrl(clusterData.getBrokerServiceUrl());
    } else {
      clientBuilder.serviceUrl(clusterData.getServiceUrl());
    }

    return clientBuilder.build();
  }

  private ClusterData retrieveClusterData() throws PulsarServerException {
    if (pulsarResources == null) {
      throw new PulsarServerException(
          "Failed to retrieve Cluster data due to empty ConfigurationStoreServers");
    }
    try {
      String path = "/admin/clusters/" + config.getClusterName();
      return localCluster =
          pulsarResources
              .getClusterResources()
              .get(path)
              .orElseThrow(() -> new MetadataStoreException.NotFoundException(path));
    } catch (Exception e) {
      throw new PulsarServerException(e);
    }
  }

  public void awaitTermination() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
    if (tlsServer != null) {
      tlsServer.awaitTermination();
    }
  }

  public void close() throws InterruptedException {
    if (server != null) {
      server.shutdown();
    }
    if (tlsServer != null) {
      tlsServer.shutdown();
    }
    if (server != null) {
      server.awaitTermination(30, TimeUnit.SECONDS);
    }
    if (tlsServer != null) {
      tlsServer.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public ScheduledExecutorService getExecutor() {
    return executor;
  }

  public boolean isAuthorizationEnabled() {
    if (this.config == null) return false;
    return this.config.isAuthorizationEnabled();
  }

  public AuthorizationService getAuthorizationService() {
    return authorizationService;
  }

  public Optional<Integer> getListenPort() {
    if (server != null) {
      return Optional.of(server.getPort());
    } else {
      return Optional.empty();
    }
  }

  public Optional<Integer> getListenPortTLS() {
    if (tlsServer != null) {
      return Optional.of(tlsServer.getPort());
    } else {
      return Optional.empty();
    }
  }
}
