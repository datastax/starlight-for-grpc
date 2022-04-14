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

import static com.datastax.oss.starlight.grpc.Constants.AUTHORIZATION_METADATA_KEY;
import static com.datastax.oss.starlight.grpc.Constants.CLIENT_PARAMS_METADATA_KEY;
import static org.apache.pulsar.broker.resources.PulsarResources.createMetadataStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ProducerRequest;
import com.datastax.oss.starlight.grpc.proto.ProducerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerSend;
import com.datastax.oss.starlight.grpc.proto.PulsarGrpc;
import com.datastax.oss.starlight.grpc.utils.PulsarCluster;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prometheus.client.CollectorRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationMetadataCacheService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class AuthorizationTest {
  private static final String TOPIC = "test-topic";

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;

  private static final SecretKey SECRET_KEY =
      AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
  private static final String CLIENT_ROLE = "client";
  private static ManagedChannel channel;

  private static PulsarGrpc.PulsarStub stub;

  @BeforeAll
  public static void before() throws Exception {
    CollectorRegistry.defaultRegistry.clear();
    cluster = new PulsarCluster(tempDir);
    cluster.start();

    cluster
        .getService()
        .getAdminClient()
        .namespaces()
        .grantPermissionOnNamespace(
            "public/default",
            CLIENT_ROLE,
            new HashSet<>(Arrays.asList(AuthAction.consume, AuthAction.produce)));

    GatewayConfiguration config = new GatewayConfiguration();
    config.setBrokerServiceURL(cluster.getAddress());
    config.setBrokerWebServiceURL(cluster.getAddress());
    int port = PortManager.nextFreePort();
    config.setGrpcServicePort(port);
    config.setConfigurationStoreServers(
        cluster.getService().getConfig().getConfigurationStoreServers());

    config.setAuthenticationEnabled(true);
    config.setAuthenticationProviders(Sets.newHashSet("org.apache.pulsar.broker.authentication.AuthenticationProviderToken"));
    config
        .getProperties()
        .setProperty(
            "tokenSecretKey",
            "data:;base64," + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));

    config.setAuthorizationEnabled(true);

    MetadataStoreExtended configMetadataStore = createMetadataStore(
        config.getConfigurationStoreServers(), config.getZookeeperSessionTimeoutMs());
    PulsarResources pulsarResources = new PulsarResources(null, configMetadataStore);
    ConfigurationMetadataCacheService configurationCacheService =
        new ConfigurationMetadataCacheService(pulsarResources, null);
    AuthorizationService authorizationService = new AuthorizationService(
        ConfigurationUtils.convertFrom(config), configurationCacheService);

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
            authorizationService,
            clusterData);
    gatewayService.start();

    channel =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPort().orElse(-1))
            .usePlaintext()
            .build();
    stub = PulsarGrpc.newStub(channel);
  }

  @AfterAll
  public static void after() throws Exception {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(30, TimeUnit.SECONDS);
    }
    if (cluster != null) {
      cluster.close();
    }
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @Test
  void testAuthorizationSuccess() throws Exception {
    String clientToken =
        Jwts.builder().setSubject(CLIENT_ROLE).signWith(SECRET_KEY).compact();
    callProduce(clientToken);
  }

  @Test
  void testAuthorizationFailure() throws Exception {
    try {
      String unauthorizedUser =
          Jwts.builder().setSubject("unauthorized").signWith(SECRET_KEY).compact();
      callProduce(unauthorizedUser);
      fail("Should have thrown StatusRuntimeException");
    } catch (ExecutionException ex) {
      StatusRuntimeException e = (StatusRuntimeException) ex.getCause();
      assertEquals(e.getStatus().getCode(), Status.Code.PERMISSION_DENIED);
      assertEquals("Failed to create producer: Not authorized", e.getStatus().getDescription());
    }
  }

  private void callProduce(String unauthorized_user)
      throws ExecutionException, InterruptedException, TimeoutException {
    ClientParameters clientParams = ClientParameters.newBuilder().setTopic(TOPIC).build();
    Metadata headers = new Metadata();
    headers.put(CLIENT_PARAMS_METADATA_KEY, clientParams.toByteArray());
    headers.put(AUTHORIZATION_METADATA_KEY, "Bearer " + unauthorized_user);

    PulsarGrpc.PulsarStub pulsarStub = MetadataUtils.attachHeaders(stub, headers);
    CompletableFuture<ProducerResponse> response = new CompletableFuture<>();
    StreamObserver<ProducerResponse> responses =
        new StreamObserver<ProducerResponse>() {
          @Override
          public void onNext(ProducerResponse consumerResponse) {
            response.complete(consumerResponse);
          }

          @Override
          public void onError(Throwable throwable) {
            response.completeExceptionally(throwable);
          }

          @Override
          public void onCompleted() {
          }
        };
    StreamObserver<ProducerRequest> request = pulsarStub.produce(responses);
    request.onNext(
        ProducerRequest.newBuilder().setSend(ProducerSend.newBuilder().setPayload(ByteString.copyFrom("test".getBytes(
            StandardCharsets.UTF_8)))).build());
    response.get(5, TimeUnit.SECONDS);
  }
}
