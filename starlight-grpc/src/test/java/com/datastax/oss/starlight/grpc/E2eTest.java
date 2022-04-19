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

import static com.datastax.oss.starlight.grpc.Constants.CLIENT_PARAMS_METADATA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerMessage;
import com.datastax.oss.starlight.grpc.proto.ConsumerParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerRequest;
import com.datastax.oss.starlight.grpc.proto.ProducerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerSend;
import com.datastax.oss.starlight.grpc.proto.PulsarGrpc;
import com.datastax.oss.starlight.grpc.proto.ReaderPermits;
import com.datastax.oss.starlight.grpc.proto.ReaderRequest;
import com.datastax.oss.starlight.grpc.proto.ReaderResponse;
import com.datastax.oss.starlight.grpc.utils.PulsarCluster;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class E2ETest {
  private static final String TOPIC = "test-topic";

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;
  private ManagedChannel channel1;
  private ManagedChannel channel2;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();

    GatewayConfiguration config = new GatewayConfiguration();
    config.setBrokerServiceURL(cluster.getAddress());
    config.setBrokerWebServiceURL(cluster.getAddress());
    int port = PortManager.nextFreePort();
    config.setGrpcServicePort(port);
    config.setConfigurationStoreServers(
        cluster.getService().getConfig().getConfigurationStoreServers());

    gatewayService =
        new GatewayService(
            config,
            new AuthenticationService(ConfigurationUtils.convertFrom(config)),
            null,
            cluster.getClusterData());
    gatewayService.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @BeforeEach
  void setUp() {
    channel1 =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPort().orElse(-1))
            .usePlaintext()
            .build();
    channel2 =
        NettyChannelBuilder.forAddress("localhost", gatewayService.getListenPort().orElse(-1))
            .usePlaintext()
            .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    channel1.shutdownNow();
    channel2.shutdownNow();
    channel1.awaitTermination(30, TimeUnit.SECONDS);
    channel2.awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test
  void testSendAndConsume() throws Exception {
    ClientParameters consumerParameters =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setConsumerParameters(
                ConsumerParameters.newBuilder().setSubscription("test-subscription"))
            .build();

    Metadata consumerHeaders = new Metadata();
    consumerHeaders.put(CLIENT_PARAMS_METADATA_KEY, consumerParameters.toByteArray());

    PulsarGrpc.PulsarStub consumerStub =
        MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel1), consumerHeaders);

    LinkedBlockingQueue<ConsumerMessage> receivedMessages = new LinkedBlockingQueue<>();
    CountDownLatch subscribed = new CountDownLatch(1);
    StreamObserver<ConsumerResponse> consumerResponses =
        new StreamObserver<ConsumerResponse>() {
          @Override
          public void onNext(ConsumerResponse consumerResponse) {
            if (consumerResponse.hasSubscribeSuccess()) {
              subscribed.countDown();
            } else {
              receivedMessages.add(consumerResponse.getMessage());
            }
          }

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        };
    consumerStub.consume(consumerResponses);
    assertTrue(subscribed.await(5, TimeUnit.SECONDS));

    ClientParameters producerParameters = ClientParameters.newBuilder().setTopic(TOPIC).build();

    Metadata producerHeaders = new Metadata();
    producerHeaders.put(CLIENT_PARAMS_METADATA_KEY, producerParameters.toByteArray());

    PulsarGrpc.PulsarStub producerStub =
        MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel2), producerHeaders);

    StreamObserver<ProducerResponse> producerResponses =
        new StreamObserver<ProducerResponse>() {
          @Override
          public void onNext(ProducerResponse producerResponse) {}

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ProducerRequest> producerRequests = producerStub.produce(producerResponses);

    for (int i = 0; i < 10; i++) {
      producerRequests.onNext(
          ProducerRequest.newBuilder()
              .setSend(
                  ProducerSend.newBuilder()
                      .setPayload(
                          ByteString.copyFrom(("test-" + i).getBytes(StandardCharsets.UTF_8))))
              .build());
    }

    for (int i = 0; i < 10; i++) {
      ConsumerMessage message = receivedMessages.poll(60, TimeUnit.SECONDS);
      assertNotNull(message);
      assertEquals(
          ByteString.copyFrom(("test-" + i).getBytes(StandardCharsets.UTF_8)),
          message.getPayload());
    }
  }

  @Test
  void testSendAndRead() throws Exception {
    ClientParameters readerParameters = ClientParameters.newBuilder().setTopic(TOPIC).build();

    Metadata readerHeaders = new Metadata();
    readerHeaders.put(CLIENT_PARAMS_METADATA_KEY, readerParameters.toByteArray());

    PulsarGrpc.PulsarStub readerStub =
        MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel1), readerHeaders);

    LinkedBlockingQueue<ConsumerMessage> receivedMessages = new LinkedBlockingQueue<>();
    CountDownLatch subscribed = new CountDownLatch(1);
    StreamObserver<ReaderResponse> readerResponses =
        new StreamObserver<ReaderResponse>() {
          @Override
          public void onNext(ReaderResponse readerResponse) {
            if (readerResponse.hasReaderSuccess()) {
              subscribed.countDown();
            } else {
              receivedMessages.add(readerResponse.getMessage());
            }
          }

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ReaderRequest> requests = readerStub.read(readerResponses);
    assertTrue(subscribed.await(5, TimeUnit.SECONDS));

    ClientParameters producerParameters = ClientParameters.newBuilder().setTopic(TOPIC).build();

    Metadata producerHeaders = new Metadata();
    producerHeaders.put(CLIENT_PARAMS_METADATA_KEY, producerParameters.toByteArray());

    PulsarGrpc.PulsarStub producerStub =
        MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel2), producerHeaders);

    StreamObserver<ProducerResponse> producerResponses =
        new StreamObserver<ProducerResponse>() {
          @Override
          public void onNext(ProducerResponse producerResponse) {}

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ProducerRequest> producerRequests = producerStub.produce(producerResponses);

    for (int i = 0; i < 10; i++) {
      producerRequests.onNext(
          ProducerRequest.newBuilder()
              .setSend(
                  ProducerSend.newBuilder()
                      .setPayload(
                          ByteString.copyFrom(("test-" + i).getBytes(StandardCharsets.UTF_8))))
              .build());
    }

    for (int i = 0; i < 2; i++) {
      assertNull(receivedMessages.poll(1, TimeUnit.SECONDS));
      requests.onNext(
          ReaderRequest.newBuilder().setPermits(ReaderPermits.newBuilder().setPermits(5)).build());

      for (int j = 0; j < 5; j++) {
        ConsumerMessage message = receivedMessages.poll(1, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(
            ByteString.copyFrom(("test-" + (i * 5 + j)).getBytes(StandardCharsets.UTF_8)),
            message.getPayload());
      }
    }
  }
}
