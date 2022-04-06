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

import static com.datastax.oss.starlight.grpc.Constants.CLIENT_PARAMS_CTX_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConsumerHandlerTest {
  private static final String TOPIC = "test-topic";
  public static final String TEST_SUBSCRIPTION = "test-subscription";

  private GatewayService gatewayService;
  private ConsumerBuilderImpl<byte[]> consumerBuilder;
  private ConsumerBase<byte[]> consumer;

  @BeforeEach
  void setup() throws Exception {
    gatewayService = mock(GatewayService.class);
    PulsarClient pulsarClient = mock(PulsarClient.class);
    consumerBuilder = spy(new ConsumerBuilderImpl<>(null, null));
    consumer = mock(ConsumerBase.class);

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
  void testParameters() throws Exception {
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
    MessageIdImpl messageId = new MessageIdImpl(1, 2, 3);

    StreamObserver<ConsumerRequest> requests = callConsume();

    ConsumerAck.Builder ack =
        ConsumerAck.newBuilder().setMessageId(ByteString.copyFrom(messageId.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setAck(ack).build());

    verify(consumer, times(1)).acknowledgeAsync(messageId);
  }

  @Test
  void testAckInMap() {
    MessageIdImpl inCacheMessageId = new MessageIdImpl(1, 2, 3);
    MessageIdImpl ackMessageId = new MessageIdImpl(4, 5, 6);

    ConsumerHandler consumerHandler = newTestConsumerHandler(new CompletableFuture<>());
    consumerHandler
        .getMessageIdCache()
        .put(ByteString.copyFrom(ackMessageId.toByteArray()), inCacheMessageId);
    StreamObserver<ConsumerRequest> requests = consumerHandler.consume();

    ConsumerAck.Builder ack =
        ConsumerAck.newBuilder().setMessageId(ByteString.copyFrom(ackMessageId.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setAck(ack).build());

    verify(consumer, times(1)).acknowledgeAsync(inCacheMessageId);
    verify(consumer, never()).acknowledgeAsync(ackMessageId);
    assertEquals(0, consumerHandler.getMessageIdCache().size());
  }

  @Test
  void testNackNotInMap() {
    MessageIdImpl messageId = new MessageIdImpl(1, 2, 3);

    StreamObserver<ConsumerRequest> requests = callConsume();

    ConsumerNack.Builder ack =
        ConsumerNack.newBuilder().setMessageId(ByteString.copyFrom(messageId.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setNack(ack).build());

    verify(consumer, times(1)).negativeAcknowledge(messageId);
  }

  @Test
  void testNackInMap() {
    MessageIdImpl inCacheMessageId = new MessageIdImpl(1, 2, 3);
    MessageIdImpl ackMessageId = new MessageIdImpl(4, 5, 6);

    ConsumerHandler consumerHandler = newTestConsumerHandler(new CompletableFuture<>());
    consumerHandler
        .getMessageIdCache()
        .put(ByteString.copyFrom(ackMessageId.toByteArray()), inCacheMessageId);
    StreamObserver<ConsumerRequest> requests = consumerHandler.consume();

    ConsumerNack.Builder nack =
        ConsumerNack.newBuilder().setMessageId(ByteString.copyFrom(ackMessageId.toByteArray()));
    requests.onNext(ConsumerRequest.newBuilder().setNack(nack).build());

    verify(consumer, times(1)).negativeAcknowledge(inCacheMessageId);
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
            response.complete(consumerResponse);
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
}
