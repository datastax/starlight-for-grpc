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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerAck;
import com.datastax.oss.starlight.grpc.proto.ConsumerEndOfTopic;
import com.datastax.oss.starlight.grpc.proto.ConsumerEndOfTopicResponse;
import com.datastax.oss.starlight.grpc.proto.ConsumerMessage;
import com.datastax.oss.starlight.grpc.proto.ConsumerNack;
import com.datastax.oss.starlight.grpc.proto.ConsumerParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerRequest;
import com.datastax.oss.starlight.grpc.proto.ConsumerResponse;
import com.datastax.oss.starlight.grpc.proto.ConsumerUnsubscribe;
import com.datastax.oss.starlight.grpc.proto.DeadLetterPolicy;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
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
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConsumerHandlerTest {
  private static final String TOPIC = "test-topic";
  private static final String TEST_SUBSCRIPTION = "test-subscription";
  private static final String TEST_ROLE = "test-role";
  private static final MessageId TEST_MESSAGE_ID = new MessageIdImpl(1, 2, 3);

  private GatewayService gatewayService;
  private ConsumerBuilderImpl<byte[]> consumerBuilder;
  private ConsumerBase<byte[]> consumer;

  @BeforeEach
  void setup() throws Exception {
    gatewayService = mock(GatewayService.class);
    PulsarClient pulsarClient = mock(PulsarClient.class);
    consumerBuilder = spy(new ConsumerBuilderImpl<>(null, null));
    consumer = mock(ConsumerBase.class);

    when(gatewayService.getExecutor()).thenReturn(Executors.newSingleThreadScheduledExecutor());
    when(gatewayService.getPulsarClient()).thenReturn(pulsarClient);
    when(pulsarClient.newConsumer()).thenReturn(consumerBuilder);
    doReturn(consumer).when(consumerBuilder).subscribe();
    when(consumer.receiveAsync()).thenReturn(new CompletableFuture<>());
    when(consumer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setConsumerParameters(
                ConsumerParameters.newBuilder().setSubscription(TEST_SUBSCRIPTION))
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();
  }

  @Test
  void testParameters() {
    long ackTimeout = 1042;
    int receiverQueueSize = 999;
    String consumerName = "test-consumer";
    int priorityLevel = 44;
    String dlTopic = "test-dl-topic";
    int dlMaxRedeliverCount = 45;
    int negativeAckRedeliveryDelayMillis = 46;

    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setConsumerParameters(
                ConsumerParameters.newBuilder()
                    .setSubscription(TEST_SUBSCRIPTION)
                    .setAckTimeoutMillis(UInt64Value.of(ackTimeout))
                    .setSubscriptionType(
                        ConsumerParameters.SubscriptionType.SUBSCRIPTION_TYPE_SHARED)
                    .setReceiverQueueSize(UInt32Value.of(receiverQueueSize))
                    .setConsumerName(consumerName)
                    .setPriorityLevel(UInt32Value.of(priorityLevel))
                    .setNegativeAckRedeliveryDelayMillis(
                        UInt64Value.of(negativeAckRedeliveryDelayMillis))
                    .setDeadLetterPolicy(
                        DeadLetterPolicy.newBuilder()
                            .setDeadLetterTopic(dlTopic)
                            .setMaxRedeliverCount(UInt32Value.of(dlMaxRedeliverCount)))
                    .setCryptoFailureAction(
                        ConsumerParameters.ConsumerCryptoFailureAction
                            .CONSUMER_CRYPTO_FAILURE_ACTION_DISCARD))
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    callConsume();

    ConsumerConfigurationData<byte[]> consumerBuilderConf = consumerBuilder.getConf();
    assertEquals(ackTimeout, consumerBuilderConf.getAckTimeoutMillis());
    assertEquals(SubscriptionType.Shared, consumerBuilderConf.getSubscriptionType());
    assertEquals(receiverQueueSize, consumerBuilderConf.getReceiverQueueSize());
    assertEquals(consumerName, consumerBuilderConf.getConsumerName());
    assertEquals(priorityLevel, consumerBuilderConf.getPriorityLevel());
    assertEquals(dlTopic, consumerBuilderConf.getDeadLetterPolicy().getDeadLetterTopic());
    assertEquals(
        dlMaxRedeliverCount, consumerBuilderConf.getDeadLetterPolicy().getMaxRedeliverCount());
    assertEquals(
        negativeAckRedeliveryDelayMillis * 1000,
        consumerBuilderConf.getNegativeAckRedeliveryDelayMicros());
    assertEquals(ConsumerCryptoFailureAction.DISCARD, consumerBuilderConf.getCryptoFailureAction());

    // Test generated DLQ topic name
    clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setConsumerParameters(
                ConsumerParameters.newBuilder()
                    .setSubscription(TEST_SUBSCRIPTION)
                    .setReceiverQueueSize(UInt32Value.of(1001))
                    .setDeadLetterPolicy(
                        DeadLetterPolicy.newBuilder()
                            .setMaxRedeliverCount(UInt32Value.of(dlMaxRedeliverCount))))
            .build();

    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    callConsume();

    assertEquals(
        "persistent://public/default/test-topic-test-subscription-DLQ",
        consumerBuilderConf.getDeadLetterPolicy().getDeadLetterTopic());
    // receive queue size is the minimum value of default value (1000) and user defined value(1001)
    assertEquals(1000, consumerBuilderConf.getReceiverQueueSize());
  }

  @Test
  void testMissingClientParameters() {
    Context.current().detach(Context.ROOT);
    try {
      callConsume();
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
      callConsume();
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Missing topic parameter", e.getMessage());
    }
  }

  @Test
  void testEmptySubscriptionParameter() {
    ClientParameters clientParams = ClientParameters.newBuilder().setTopic(TOPIC).build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    try {
      callConsume();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);
      assertEquals(
          "Invalid header params: Empty subscription name", e.getStatus().getDescription());
    }
  }

  @Test
  void testInvalidParameter() {
    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setConsumerParameters(
                ConsumerParameters.newBuilder()
                    .setSubscription(TEST_SUBSCRIPTION)
                    .setReceiverQueueSize(UInt32Value.of(-1))
                    .build())
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    try {
      callConsume();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);
      assertTrue(e.getStatus().getDescription().startsWith("Invalid header params: "));
    }
  }

  @Test
  void testSubscribeSuccess() throws Exception {
    CountDownLatch subscribed = new CountDownLatch(1);
    StreamObserver<ConsumerResponse> responses =
        new StreamObserver<ConsumerResponse>() {
          @Override
          public void onNext(ConsumerResponse consumerResponse) {
            if (consumerResponse.hasSubscribeSuccess()) {
              subscribed.countDown();
            }
          }

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        };
    new ConsumerHandler(gatewayService, responses).consume();
    assertTrue(subscribed.await(5, TimeUnit.SECONDS));
  }

  @Test
  void testSubscribeException() throws Exception {
    doThrow(new PulsarClientException.TimeoutException("timeout"))
        .when(consumerBuilder)
        .subscribe();
    try {
      callConsume();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.ABORTED);
      assertEquals("Failed to subscribe: timeout", e.getStatus().getDescription());
    }
  }

  @Test
  void testOnCompleted() {
    StreamObserver<ConsumerRequest> requests = callConsume();
    requests.onCompleted();

    verify(consumer, times(1)).closeAsync();
  }

  @Test
  void testOnError() {
    StreamObserver<ConsumerRequest> requests = callConsume();
    requests.onError(new RuntimeException());

    verify(consumer, times(1)).closeAsync();
  }

  @Test
  void testAckNotInMap() {
    StreamObserver<ConsumerRequest> requests = callConsume();

    ConsumerAck.Builder ack =
        ConsumerAck.newBuilder().setMessageId(ByteString.copyFrom(TEST_MESSAGE_ID.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setAck(ack).build());

    verify(consumer, times(1)).acknowledgeAsync(TEST_MESSAGE_ID);
  }

  @Test
  void testAckInMap() {
    MessageId ackMessageId = new MessageIdImpl(4, 5, 6);

    ConsumerHandler consumerHandler = newTestConsumerHandler(new CompletableFuture<>());
    consumerHandler
        .getMessageIdCache()
        .put(ByteString.copyFrom(ackMessageId.toByteArray()), TEST_MESSAGE_ID);
    StreamObserver<ConsumerRequest> requests = consumerHandler.consume();

    ConsumerAck.Builder ack =
        ConsumerAck.newBuilder().setMessageId(ByteString.copyFrom(ackMessageId.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setAck(ack).build());

    verify(consumer, times(1)).acknowledgeAsync(TEST_MESSAGE_ID);
    verify(consumer, never()).acknowledgeAsync(ackMessageId);
    assertEquals(0, consumerHandler.getMessageIdCache().size());
  }

  @Test
  void testNackNotInMap() {
    StreamObserver<ConsumerRequest> requests = callConsume();

    ConsumerNack.Builder ack =
        ConsumerNack.newBuilder().setMessageId(ByteString.copyFrom(TEST_MESSAGE_ID.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setNack(ack).build());

    verify(consumer, times(1)).negativeAcknowledge(TEST_MESSAGE_ID);
  }

  @Test
  void testNackInMap() {
    MessageId ackMessageId = new MessageIdImpl(4, 5, 6);

    ConsumerHandler consumerHandler = newTestConsumerHandler(new CompletableFuture<>());
    consumerHandler
        .getMessageIdCache()
        .put(ByteString.copyFrom(ackMessageId.toByteArray()), TEST_MESSAGE_ID);
    StreamObserver<ConsumerRequest> requests = consumerHandler.consume();

    ConsumerNack.Builder nack =
        ConsumerNack.newBuilder().setMessageId(ByteString.copyFrom(ackMessageId.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setNack(nack).build());

    verify(consumer, times(1)).negativeAcknowledge(TEST_MESSAGE_ID);
    verify(consumer, never()).negativeAcknowledge(ackMessageId);
    assertEquals(0, consumerHandler.getMessageIdCache().size());
  }

  @Test
  void testUnsubscribe() {
    StreamObserver<ConsumerRequest> requests = callConsume();

    requests.onNext(
        ConsumerRequest.newBuilder()
            .setUnsubscribe(ConsumerUnsubscribe.getDefaultInstance())
            .build());

    verify(consumer, times(1)).unsubscribeAsync();
  }

  @Test
  void testEndOfTopic() throws Exception {
    when(consumer.hasReachedEndOfTopic()).thenReturn(true);

    CompletableFuture<ConsumerResponse> response = new CompletableFuture<>();
    StreamObserver<ConsumerRequest> requests = callConsume(response);

    requests.onNext(
        ConsumerRequest.newBuilder()
            .setEndOfTopic(ConsumerEndOfTopic.getDefaultInstance())
            .build());

    ConsumerEndOfTopicResponse endOfTopicResponse =
        response.get(5, TimeUnit.SECONDS).getEndOfTopicResponse();
    assertNotNull(endOfTopicResponse);
    assertTrue(endOfTopicResponse.getReachedEndOfTopic());
  }

  @Test
  void testConsumeError() {
    CompletableFuture<ConsumerResponse> responses = new CompletableFuture<>();
    StreamObserver<ConsumerRequest> requests = callConsume(responses);

    ConsumerAck.Builder ack =
        ConsumerAck.newBuilder().setMessageId(ByteString.copyFromUtf8("invalid"));
    requests.onNext(ConsumerRequest.newBuilder().setAck(ack).build());
    assertTrue(responses.isCompletedExceptionally());
    verify(consumer, times(1)).closeAsync();
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

    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    LinkedBlockingQueue<ConsumerResponse> responses = new LinkedBlockingQueue<>();

    StreamObserver<ConsumerResponse> responseStreamObserver =
        new StreamObserver<ConsumerResponse>() {
          @Override
          public void onNext(ConsumerResponse consumerResponse) {
            responses.add(consumerResponse);
          }

          @Override
          public void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
          }

          @Override
          public void onCompleted() {}
        };
    new ConsumerHandler(gatewayService, responseStreamObserver).consume();

    ConsumerResponse consumerResponse = responses.poll(5, TimeUnit.SECONDS);
    assertNotNull(consumerResponse);
    assertTrue(consumerResponse.hasSubscribeSuccess());

    consumerResponse = responses.poll(5, TimeUnit.SECONDS);
    assertNotNull(consumerResponse);
    ConsumerMessage consumerMessage = consumerResponse.getMessage();
    assertNotNull(consumerMessage);
    assertArrayEquals(TEST_MESSAGE_ID.toByteArray(), consumerMessage.getMessageId().toByteArray());
    assertEquals(payload, consumerMessage.getPayload().toStringUtf8());
    assertEquals(1, consumerMessage.getPropertiesCount());
    assertEquals("test-property-value", consumerMessage.getPropertiesOrThrow("test-property-key"));
    assertEquals(publishTime, consumerMessage.getPublishTime());
    assertEquals(eventTime, consumerMessage.getEventTime());
    assertEquals(key, consumerMessage.getKey());

    // Check that we receive more messages
    for (int i = 0; i < 100; i++) {
      consumerResponse = responses.poll(5, TimeUnit.SECONDS);
      assertNotNull(consumerResponse);
      assertTrue(consumerResponse.hasMessage());
    }
  }

  @Test
  void testReceiveMessageReceiveQueue() throws Exception {
    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setConsumerParameters(
                ConsumerParameters.newBuilder()
                    .setSubscription(TEST_SUBSCRIPTION)
                    .setReceiverQueueSize(UInt32Value.of(0)))
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    TestMessage message = new TestMessage(TEST_MESSAGE_ID);
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    LinkedBlockingQueue<ConsumerResponse> responses = new LinkedBlockingQueue<>();

    StreamObserver<ConsumerResponse> responseStreamObserver =
        new StreamObserver<ConsumerResponse>() {
          @Override
          public void onNext(ConsumerResponse consumerResponse) {
            responses.add(consumerResponse);
          }

          @Override
          public void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
          }

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ConsumerRequest> requests =
        new ConsumerHandler(gatewayService, responseStreamObserver).consume();

    ConsumerResponse consumerResponse = responses.poll(5, TimeUnit.SECONDS);
    assertNotNull(consumerResponse);
    assertTrue(consumerResponse.hasSubscribeSuccess());

    consumerResponse = responses.poll(5, TimeUnit.SECONDS);
    assertNotNull(consumerResponse);
    assertNotNull(consumerResponse.getMessage());

    assertNull(responses.poll(1, TimeUnit.SECONDS));

    requests.onNext(
        ConsumerRequest.newBuilder()
            .setAck(
                ConsumerAck.newBuilder()
                    .setMessageId(ByteString.copyFrom(TEST_MESSAGE_ID.toByteArray())))
            .build());

    consumerResponse = responses.poll(1, TimeUnit.SECONDS);
    assertNotNull(consumerResponse);
    assertNotNull(consumerResponse.getMessage());
  }

  @Test
  void testReceiveMessageException() {
    CompletableFuture<Message<byte[]>> messageException = new CompletableFuture<>();
    messageException.completeExceptionally(new PulsarClientException("error"));
    when(consumer.receiveAsync()).thenReturn(messageException);

    CompletableFuture<ConsumerResponse> response = new CompletableFuture<>();
    callConsume(response);

    assertTrue(response.isCompletedExceptionally());
  }

  @Test
  void testAuthorizedUser() throws Exception {
    Context.current().withValue(AUTHENTICATION_ROLE_CTX_KEY, TEST_ROLE).attach();
    when(gatewayService.isAuthorizationEnabled()).thenReturn(true);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    when(gatewayService.getAuthorizationService()).thenReturn(authorizationService);
    when(authorizationService.canConsume(
            eq(TopicName.get(TOPIC)), eq(TEST_ROLE), any(), eq(TEST_SUBSCRIPTION)))
        .thenReturn(true);

    callConsume();
  }

  @Test
  void testUnAuthorizedUser() {
    when(gatewayService.isAuthorizationEnabled()).thenReturn(true);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    when(gatewayService.getAuthorizationService()).thenReturn(authorizationService);

    try {
      callConsume();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.PERMISSION_DENIED);
      assertEquals("Failed to subscribe: Not authorized", e.getStatus().getDescription());
    }
  }

  @Test
  void testAuthorizationException() throws Exception {
    Context.current().withValue(AUTHENTICATION_ROLE_CTX_KEY, TEST_ROLE).attach();
    when(gatewayService.isAuthorizationEnabled()).thenReturn(true);
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    when(gatewayService.getAuthorizationService()).thenReturn(authorizationService);
    when(authorizationService.canConsume(
            eq(TopicName.get(TOPIC)), eq(TEST_ROLE), any(), eq(TEST_SUBSCRIPTION)))
        .thenThrow(new PulsarClientException.TimeoutException("timeout"));

    try {
      callConsume();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.ABORTED);
      assertEquals("Failed to subscribe: timeout", e.getStatus().getDescription());
    }
  }

  private StreamObserver<ConsumerRequest> callConsume() {
    return callConsume(new CompletableFuture<>());
  }

  private StreamObserver<ConsumerRequest> callConsume(
      CompletableFuture<ConsumerResponse> response) {
    return newTestConsumerHandler(response).consume();
  }

  private ConsumerHandler newTestConsumerHandler(CompletableFuture<ConsumerResponse> response) {
    StreamObserver<ConsumerResponse> responses =
        new StreamObserver<ConsumerResponse>() {
          @Override
          public void onNext(ConsumerResponse consumerResponse) {
            if (!consumerResponse.hasSubscribeSuccess()) {
              response.complete(consumerResponse);
            }
          }

          @Override
          public void onError(Throwable throwable) {
            response.completeExceptionally(throwable);
          }

          @Override
          public void onCompleted() {}
        };
    return new ConsumerHandler(gatewayService, responses);
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
