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

import static com.datastax.oss.starlight.grpc.Constants.AUTHENTICATION_ROLE_CTX_KEY;
import static com.datastax.oss.starlight.grpc.Constants.CLIENT_PARAMS_CTX_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerMessage;
import com.datastax.oss.starlight.grpc.proto.ReaderParameters;
import com.datastax.oss.starlight.grpc.proto.ReaderPermits;
import com.datastax.oss.starlight.grpc.proto.ReaderRequest;
import com.datastax.oss.starlight.grpc.proto.ReaderResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ReaderBuilderImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReaderHandlerTest {
  private static final String TOPIC = "test-topic";
  private static final String TEST_SUBSCRIPTION = "test-subscription";
  private static final String TEST_ROLE = "test-role";
  private static final MessageId TEST_MESSAGE_ID = new MessageIdImpl(1, 2, 3);

  private GatewayService gatewayService;
  private ReaderBuilderImpl<byte[]> readerBuilder;
  private ReaderImpl<byte[]> reader;

  @BeforeEach
  void setup() throws Exception {
    gatewayService = mock(GatewayService.class);
    PulsarClient pulsarClient = mock(PulsarClient.class);
    readerBuilder = spy(new ReaderBuilderImpl<>(null, null));
    reader = mock(ReaderImpl.class);

    when(gatewayService.getExecutor()).thenReturn(Executors.newSingleThreadScheduledExecutor());
    when(gatewayService.getPulsarClient()).thenReturn(pulsarClient);
    when(pulsarClient.newReader()).thenReturn(readerBuilder);
    doReturn(reader).when(readerBuilder).create();
    ConsumerImpl<byte[]> consumer = mock(ConsumerImpl.class);
    when(consumer.getSubscription()).thenReturn(TEST_SUBSCRIPTION);
    when(reader.getConsumer()).thenReturn(consumer);
    when(reader.readNextAsync()).thenReturn(new CompletableFuture<>());
    when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

    ClientParameters clientParams = ClientParameters.newBuilder().setTopic(TOPIC).build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();
  }

  @Test
  void testParameters() {
    int receiverQueueSize = 999;
    String readerName = "test-reader";
    MessageId messageId = new MessageIdImpl(1, 2, 3);

    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setReaderParameters(
                ReaderParameters.newBuilder()
                    .setMessageId(ByteString.copyFrom(messageId.toByteArray()))
                    .setReceiverQueueSize(UInt32Value.of(receiverQueueSize))
                    .setReaderName(readerName)
                    .setCryptoFailureAction(
                        com.datastax.oss.starlight.grpc.proto.ConsumerCryptoFailureAction
                            .CONSUMER_CRYPTO_FAILURE_ACTION_DISCARD))
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    callRead();

    ReaderConfigurationData<byte[]> readerBuilderConf = readerBuilder.getConf();
    assertEquals(messageId, readerBuilderConf.getStartMessageId());
    assertEquals(receiverQueueSize, readerBuilderConf.getReceiverQueueSize());
    assertEquals(readerName, readerBuilderConf.getReaderName());
    assertEquals(ConsumerCryptoFailureAction.DISCARD, readerBuilderConf.getCryptoFailureAction());

    clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setReaderParameters(
                ReaderParameters.newBuilder()
                    .setReceiverQueueSize(UInt32Value.of(1001))
                    .setMessageIdMode(com.datastax.oss.starlight.grpc.proto.MessageId.EARLIEST))
            .build();

    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    callRead();
    // receive queue size is the minimum value of default value (1000) and user defined value(1001)
    assertEquals(1000, readerBuilderConf.getReceiverQueueSize());
    assertEquals(MessageId.earliest, readerBuilderConf.getStartMessageId());

    // Default values
    clientParams = ClientParameters.newBuilder().setTopic(TOPIC).build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    callRead();
    assertEquals(1000, readerBuilderConf.getReceiverQueueSize());
    assertEquals(MessageId.latest, readerBuilderConf.getStartMessageId());
  }

  @Test
  void testMissingClientParameters() {
    Context.current().detach(Context.ROOT);
    try {
      callRead();
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Missing client parameters", e.getMessage());
    }
  }

  @Test
  void testMissingTopic() {
    Context.current()
        .withValue(CLIENT_PARAMS_CTX_KEY, ClientParameters.getDefaultInstance())
        .attach();
    try {
      callRead();
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Missing topic parameter", e.getMessage());
    }
  }

  @Test
  void testInvalidParameter() {
    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setReaderParameters(
                ReaderParameters.newBuilder().setReceiverQueueSize(UInt32Value.of(-1)).build())
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    try {
      callRead();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);
      assertTrue(e.getStatus().getDescription().startsWith("Invalid header params: "));
    }
  }

  @Test
  void testCreateReaderSuccess() throws Exception {
    CountDownLatch success = new CountDownLatch(1);
    StreamObserver<ReaderResponse> responses =
        new StreamObserver<ReaderResponse>() {
          @Override
          public void onNext(ReaderResponse readerResponse) {
            if (readerResponse.hasReaderSuccess()) {
              success.countDown();
            }
          }

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        };
    new ReaderHandler(gatewayService, responses).read();
    assertTrue(success.await(5, TimeUnit.SECONDS));
  }

  @Test
  void testCreateReaderException() throws Exception {
    doThrow(new PulsarClientException.TimeoutException("timeout")).when(readerBuilder).create();
    try {
      callRead();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.ABORTED);
      assertEquals("Failed to create reader: timeout", e.getStatus().getDescription());
    }
  }

  @Test
  void testOnCompleted() {
    StreamObserver<ReaderRequest> requests = callRead();
    requests.onCompleted();

    verify(reader, times(1)).closeAsync();
  }

  @Test
  void testOnError() {
    StreamObserver<ReaderRequest> requests = callRead();
    requests.onError(new RuntimeException());

    verify(reader, times(1)).closeAsync();
  }

  @Test
  void testReceiveMessage() throws Exception {
    String payload = "test-message";
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("test-property-key", "test-property-value");
    long publishTime = 42;
    long eventTime = 43;
    String key = "test-key";

    TestMessage message =
        new TestMessage(TEST_MESSAGE_ID, payload, properties, publishTime, eventTime, key);

    when(reader.readNextAsync()).thenReturn(CompletableFuture.completedFuture(message));

    LinkedBlockingQueue<ReaderResponse> responses = new LinkedBlockingQueue<>();

    StreamObserver<ReaderResponse> responseStreamObserver =
        new StreamObserver<ReaderResponse>() {
          @Override
          public void onNext(ReaderResponse readerResponse) {
            responses.add(readerResponse);
          }

          @Override
          public void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
          }

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ReaderRequest> request =
        new ReaderHandler(gatewayService, responseStreamObserver).read();

    ReaderResponse readerResponse = responses.poll(1, TimeUnit.SECONDS);
    assertNotNull(readerResponse);
    assertTrue(readerResponse.hasReaderSuccess());

    readerResponse = responses.poll(1, TimeUnit.SECONDS);
    assertNull(readerResponse);

    request.onNext(
        ReaderRequest.newBuilder().setPermits(ReaderPermits.newBuilder().setPermits(100)).build());

    for (int i = 0; i < 100; i++) {
      readerResponse = responses.poll(1, TimeUnit.SECONDS);
      ConsumerMessage consumerMessage = readerResponse.getMessage();
      assertNotNull(consumerMessage);
      assertArrayEquals(
          TEST_MESSAGE_ID.toByteArray(), consumerMessage.getMessageId().toByteArray());
      assertEquals(payload, consumerMessage.getPayload().toStringUtf8());
      assertEquals(1, consumerMessage.getPropertiesCount());
      assertEquals(
          "test-property-value", consumerMessage.getPropertiesOrThrow("test-property-key"));
      assertEquals(publishTime, consumerMessage.getPublishTime());
      assertEquals(eventTime, consumerMessage.getEventTime());
      assertEquals(key, consumerMessage.getKey().getValue());
    }

    readerResponse = responses.poll(1, TimeUnit.SECONDS);
    assertNull(readerResponse);
  }

  @Test
  void testAuthorizedUser() throws Exception {
    Context.current().withValue(AUTHENTICATION_ROLE_CTX_KEY, TEST_ROLE).attach();
    when(gatewayService.isAuthorizationEnabled()).thenReturn(true);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    when(gatewayService.getAuthorizationService()).thenReturn(authorizationService);
    when(authorizationService.canConsume(eq(TopicName.get(TOPIC)), eq(TEST_ROLE), any(), isNull()))
        .thenReturn(true);

    callRead();
  }

  @Test
  void testUnAuthorizedUser() {
    when(gatewayService.isAuthorizationEnabled()).thenReturn(true);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    when(gatewayService.getAuthorizationService()).thenReturn(authorizationService);

    try {
      callRead();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.PERMISSION_DENIED);
      assertEquals("Failed to create reader: Not authorized", e.getStatus().getDescription());
    }
  }

  @Test
  void testAuthorizationException() throws Exception {
    Context.current().withValue(AUTHENTICATION_ROLE_CTX_KEY, TEST_ROLE).attach();
    when(gatewayService.isAuthorizationEnabled()).thenReturn(true);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    when(gatewayService.getAuthorizationService()).thenReturn(authorizationService);
    when(authorizationService.canConsume(eq(TopicName.get(TOPIC)), eq(TEST_ROLE), any(), isNull()))
        .thenThrow(new PulsarClientException.TimeoutException("timeout"));

    try {
      callRead();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.ABORTED);
      assertEquals("Failed to create reader: timeout", e.getStatus().getDescription());
    }
  }

  private StreamObserver<ReaderRequest> callRead() {
    return callRead(new CompletableFuture<>());
  }

  private StreamObserver<ReaderRequest> callRead(CompletableFuture<ReaderResponse> response) {
    return newTestReaderHandler(response).read();
  }

  private ReaderHandler newTestReaderHandler(CompletableFuture<ReaderResponse> response) {
    StreamObserver<ReaderResponse> responses =
        new StreamObserver<ReaderResponse>() {
          @Override
          public void onNext(ReaderResponse readerResponse) {
            if (!readerResponse.hasReaderSuccess()) {
              response.complete(readerResponse);
            }
          }

          @Override
          public void onError(Throwable throwable) {
            response.completeExceptionally(throwable);
          }

          @Override
          public void onCompleted() {}
        };
    return new ReaderHandler(gatewayService, responses);
  }

  private static class TestMessage implements Message<byte[]> {
    private final MessageId messageId;
    private String payload = "";
    private Map<String, String> properties = new HashMap<>();
    private long publishTime;
    private long eventTime;
    private String key = "";

    public TestMessage(MessageId messageId) {
      this.messageId = messageId;
    }

    private TestMessage(
        MessageId messageId,
        String payload,
        Map<String, String> properties,
        long publishTime,
        long eventTime,
        String key) {
      this.messageId = messageId;
      this.payload = payload;
      this.properties = properties;
      this.publishTime = publishTime;
      this.eventTime = eventTime;
      this.key = key;
    }

    @Override
    public Map<String, String> getProperties() {
      return properties;
    }

    @Override
    public boolean hasProperty(String name) {
      return false;
    }

    @Override
    public String getProperty(String name) {
      return null;
    }

    @Override
    public byte[] getData() {
      return payload.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public byte[] getValue() {
      return null;
    }

    @Override
    public MessageId getMessageId() {
      return messageId;
    }

    @Override
    public long getPublishTime() {
      return publishTime;
    }

    @Override
    public long getEventTime() {
      return eventTime;
    }

    @Override
    public long getSequenceId() {
      return 0;
    }

    @Override
    public String getProducerName() {
      return null;
    }

    @Override
    public boolean hasKey() {
      return true;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public boolean hasBase64EncodedKey() {
      return false;
    }

    @Override
    public byte[] getKeyBytes() {
      return new byte[0];
    }

    @Override
    public boolean hasOrderingKey() {
      return false;
    }

    @Override
    public byte[] getOrderingKey() {
      return new byte[0];
    }

    @Override
    public String getTopicName() {
      return null;
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
      return Optional.empty();
    }

    @Override
    public int getRedeliveryCount() {
      return 0;
    }

    @Override
    public byte[] getSchemaVersion() {
      return new byte[0];
    }

    @Override
    public boolean isReplicated() {
      return false;
    }

    @Override
    public String getReplicatedFrom() {
      return null;
    }

    @Override
    public void release() {}
  }
}
