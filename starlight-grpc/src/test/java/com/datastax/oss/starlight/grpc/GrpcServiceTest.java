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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerParameters;
import com.datastax.oss.starlight.grpc.proto.DeadLetterPolicy;
import com.datastax.oss.starlight.grpc.proto.ProducerAck;
import com.datastax.oss.starlight.grpc.proto.ProducerParameters;
import com.datastax.oss.starlight.grpc.proto.ProducerRequest;
import com.datastax.oss.starlight.grpc.proto.ProducerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerSend;
import com.datastax.oss.starlight.grpc.proto.ProducerSendError;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.rpc.Code;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GrpcServiceTest {
  private static final String TOPIC = "test-topic";
  private static final String TEST_CONTEXT = "test-context";

  private PulsarGrpcService grpcService;
  private PulsarClient pulsarClient;
  private ProducerBuilderImpl<byte[]> producerBuilder;
  private ProducerBase<byte[]> producer;
  private TypedMessageBuilderImpl<byte[]> message;

  @BeforeEach
  void setup() throws Exception {
    GatewayService gatewayService = mock(GatewayService.class);
    grpcService = new PulsarGrpcService(gatewayService);
    pulsarClient = mock(PulsarClient.class);
    producerBuilder = spy(new ProducerBuilderImpl<>(null, null));
    producer = mock(ProducerBase.class);
    message = spy(new TypedMessageBuilderImpl<>(producer, Schema.BYTES));

    when(gatewayService.getPulsarClient()).thenReturn(pulsarClient);
    when(pulsarClient.newProducer()).thenReturn(producerBuilder);
    doReturn(producer).when(producerBuilder).create();
    when(producer.newMessage()).thenReturn(message);

    ClientParameters clientParams = ClientParameters.newBuilder().setTopic(TOPIC).build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();
  }

  @Test
  void testProduceParameters() {
    // Verify default values
    callProduce();
    ProducerConfigurationData producerBuilderConf = producerBuilder.getConf();
    assertEquals(MessageRoutingMode.SinglePartition, producerBuilderConf.getMessageRoutingMode());
    assertFalse(producerBuilderConf.isBatchingEnabled());

    String producerName = "test-producer";
    int initialSequenceId = 42;
    int sendTimeoutMillis = 43;
    int batchingMaxMessages = 44;
    int maxPendingMessages = 45;
    int batchingMaxPublishDelayMillis = 46;

    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setProducerParameters(
                ProducerParameters.newBuilder()
                    .setMessageRoutingMode(
                        ProducerParameters.MessageRoutingMode
                            .MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION)
                    .setProducerName(producerName)
                    .setInitialSequenceId(UInt64Value.of(initialSequenceId))
                    .setHashingScheme(
                        ProducerParameters.HashingScheme.HASHING_SCHEME_MURMUR3_32HASH)
                    .setSendTimeoutMillis(UInt32Value.of(sendTimeoutMillis))
                    .setBatchingEnabled(BoolValue.of(true))
                    .setBatchingMaxMessages(UInt32Value.of(batchingMaxMessages))
                    .setMaxPendingMessages(UInt32Value.of(maxPendingMessages))
                    .setBatchingMaxPublishDelayMillis(UInt64Value.of(batchingMaxPublishDelayMillis))
                    .setCompressionType(ProducerParameters.CompressionType.COMPRESSION_TYPE_LZ4))
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    callProduce();

    // Verify producer conf
    assertFalse(producerBuilderConf.isBlockIfQueueFull());
    assertEquals(producerName, producerBuilderConf.getProducerName());
    assertEquals(initialSequenceId, producerBuilderConf.getInitialSequenceId());
    assertEquals(HashingScheme.Murmur3_32Hash, producerBuilderConf.getHashingScheme());
    assertEquals(sendTimeoutMillis, producerBuilderConf.getSendTimeoutMs());
    assertTrue(producerBuilderConf.isBatchingEnabled());
    assertEquals(batchingMaxMessages, producerBuilderConf.getBatchingMaxMessages());
    assertEquals(maxPendingMessages, producerBuilderConf.getMaxPendingMessages());
    assertEquals(
        batchingMaxPublishDelayMillis * 1000,
        producerBuilderConf.getBatchingMaxPublishDelayMicros());
    assertEquals(
        MessageRoutingMode.RoundRobinPartition, producerBuilderConf.getMessageRoutingMode());
    assertEquals(CompressionType.LZ4, producerBuilderConf.getCompressionType());
    assertEquals("persistent://public/default/test-topic", producerBuilderConf.getTopicName());
  }

  @Test
  void testProduceInvalidProducerParameter() {
    ClientParameters clientParams =
        ClientParameters.newBuilder()
            .setTopic(TOPIC)
            .setProducerParameters(
                ProducerParameters.newBuilder().setBatchingMaxPublishDelayMillis(UInt64Value.of(0)))
            .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    try {
      callProduce();
      fail("Should have thrown StatusRuntimeException");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);
      assertTrue(e.getStatus().getDescription().startsWith("Invalid header params: "));
    }
  }

  @Test
  void testProduceOnCompleted() {
    doReturn(CompletableFuture.completedFuture(null)).when(producer).closeAsync();

    StreamObserver<ProducerRequest> requests = callProduce();
    requests.onCompleted();

    verify(producer, times(1)).closeAsync();
  }

  @Test
  void testProduceOnError() {
    doReturn(CompletableFuture.completedFuture(null)).when(producer).closeAsync();

    StreamObserver<ProducerRequest> requests = callProduce();
    requests.onError(new RuntimeException());

    verify(producer, times(1)).closeAsync();
  }

  @Test
  void testProduceSend() throws Exception {
    String payload = "test-payload";
    String propertyKey = "test-prop-key";
    String propertyValue = "test-prop-value";
    String key = "test-key";
    String replicationCluster = "test-cluster";
    long eventTime = 42;
    long deliverAt = 100043;
    long deliverAfterMs = 100044;

    MessageId messageId = DefaultImplementation.newMessageId(1, 2, 3);
    doReturn(CompletableFuture.completedFuture(messageId)).when(message).sendAsync();

    CompletableFuture<ProducerResponse> response = new CompletableFuture<>();
    StreamObserver<ProducerRequest> requests = callProduce(response);

    ProducerRequest request =
        ProducerRequest.newBuilder()
            .setSend(
                ProducerSend.newBuilder()
                    .setPayload(ByteString.copyFromUtf8(payload))
                    .setContext(TEST_CONTEXT)
                    .putProperties(propertyKey, propertyValue)
                    .setKey(key)
                    .addReplicationClusters(replicationCluster)
                    .setEventTime(eventTime)
                    .setDeliverAt(deliverAt))
            .build();
    requests.onNext(request);

    // Check Ack
    ProducerAck ack = response.get(5, TimeUnit.SECONDS).getAck();
    assertNotNull(ack);
    assertEquals(TEST_CONTEXT, ack.getContext());
    assertArrayEquals(messageId.toByteArray(), ack.getMessageId().toByteArray());

    // Verify message properties
    MessageImpl<byte[]> sentMessage = (MessageImpl<byte[]>) message.getMessage();
    assertEquals(new String(message.getContent().array(), StandardCharsets.UTF_8), payload);
    assertEquals(propertyValue, sentMessage.getProperty(propertyKey));
    assertEquals(key, sentMessage.getKey());
    assertIterableEquals(
        Collections.singletonList(replicationCluster), sentMessage.getReplicateTo());
    assertEquals(eventTime, sentMessage.getEventTime());
    assertEquals(deliverAt, message.getMetadataBuilder().getDeliverAtTime());

    // Test deliverAfter
    request =
        ProducerRequest.newBuilder()
            .setSend(ProducerSend.newBuilder().setDeliverAfterMs(deliverAfterMs))
            .build();
    requests.onNext(request);
    assertEquals(
        message.getMetadataBuilder().getDeliverAtTime() - System.currentTimeMillis(),
        deliverAfterMs,
        5);
  }

  @Test
  void testProduceSendException() throws Exception {
    CompletableFuture<MessageId> sendResult = new CompletableFuture<>();
    sendResult.completeExceptionally(new PulsarClientException("error"));
    doReturn(sendResult).when(message).sendAsync();

    CompletableFuture<ProducerResponse> response = new CompletableFuture<>();
    StreamObserver<ProducerRequest> requests = callProduce(response);

    ProducerRequest request =
        ProducerRequest.newBuilder()
            .setSend(
                ProducerSend.newBuilder()
                    .setPayload(ByteString.copyFromUtf8("payload"))
                    .setContext(TEST_CONTEXT))
            .build();
    requests.onNext(request);

    ProducerSendError error = response.get(5, TimeUnit.SECONDS).getError();
    assertEquals(Code.INTERNAL_VALUE, error.getStatusCode());
    assertEquals(TEST_CONTEXT, error.getContext());
    assertEquals("org.apache.pulsar.client.api.PulsarClientException: error", error.getErrorMsg());
  }

  @Test
  void testProduceSchemaSerializationException() throws Exception {
    doThrow(new SchemaSerializationException("error")).when(message).value(any());

    CompletableFuture<ProducerResponse> response = new CompletableFuture<>();
    StreamObserver<ProducerRequest> requests = callProduce(response);

    ProducerRequest request =
        ProducerRequest.newBuilder()
            .setSend(
                ProducerSend.newBuilder()
                    .setPayload(ByteString.copyFromUtf8("payload"))
                    .setContext(TEST_CONTEXT))
            .build();
    requests.onNext(request);

    ProducerSendError error = response.get(5, TimeUnit.SECONDS).getError();
    assertEquals(Code.INVALID_ARGUMENT_VALUE, error.getStatusCode());
    assertEquals(TEST_CONTEXT, error.getContext());
    assertEquals("error", error.getErrorMsg());
  }

  @Test
  void testConsumeParameters() throws Exception {
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
                    .setSubscription("test-subscription")
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

    ConsumerBuilderImpl<byte[]> consumerBuilder = spy(new ConsumerBuilderImpl<>(null, null));
    ConsumerBase<byte[]> consumer = mock(ConsumerBase.class);
    when(consumer.receiveAsync()).thenReturn(new CompletableFuture<>());
    doReturn(consumer).when(consumerBuilder).subscribe();

    when(pulsarClient.newConsumer()).thenReturn(consumerBuilder);

    grpcService.consume(null);

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
                    .setSubscription("test-subscription")
                    .setReceiverQueueSize(UInt32Value.of(1001))
                    .setDeadLetterPolicy(
                        DeadLetterPolicy.newBuilder()
                            .setMaxRedeliverCount(UInt32Value.of(dlMaxRedeliverCount))))
            .build();

    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    grpcService.consume(null);

    assertEquals(
        "persistent://public/default/test-topic-test-subscription-DLQ",
        consumerBuilderConf.getDeadLetterPolicy().getDeadLetterTopic());
    // receive queue size is the minimum value of default value (1000) and user defined value(1001)
    assertEquals(1000, consumerBuilderConf.getReceiverQueueSize());
  }

  private StreamObserver<ProducerRequest> callProduce() {
    return callProduce(new CompletableFuture<>());
  }

  private StreamObserver<ProducerRequest> callProduce(
      CompletableFuture<ProducerResponse> response) {
    StreamObserver<ProducerResponse> responses =
        new StreamObserver<ProducerResponse>() {
          @Override
          public void onNext(ProducerResponse producerResponse) {
            response.complete(producerResponse);
          }

          @Override
          public void onError(Throwable throwable) {
            response.completeExceptionally(throwable);
          }

          @Override
          public void onCompleted() {}
        };
    return grpcService.produce(responses);
  }
}
