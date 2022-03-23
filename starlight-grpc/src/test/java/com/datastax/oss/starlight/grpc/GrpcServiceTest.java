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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ProducerAck;
import com.datastax.oss.starlight.grpc.proto.ProducerParameters;
import com.datastax.oss.starlight.grpc.proto.ProducerRequest;
import com.datastax.oss.starlight.grpc.proto.ProducerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerSend;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.junit.jupiter.api.Test;

public class GrpcServiceTest {

  @Test
  void testProduce() throws Exception {

    String topic = "test-topic";
    String context = "test-context";
    String producerName = "test-producer";
    int initialSequenceId = 42;
    int sendTimeoutMillis = 43;
    int batchingMaxMessages = 44;
    int maxPendingMessages = 45;
    long batchingMaxPublishDelay = 46;

    String payload = "test-payload";
    String propertyKey = "test-prop-key";
    String propertyValue = "test-prop-value";
    String key = "test-key";
    String replicationCluster = "test-cluster";
    long eventTime = 47;
    long deliverAt = 48;

    GatewayService service = mock(GatewayService.class);
    PulsarGrpcService grpcService = new PulsarGrpcService(service);

    ClientParameters clientParams = ClientParameters.newBuilder()
        .setTopic(topic)
        .setProducerParameters(
            ProducerParameters.newBuilder()
                .setMessageRoutingMode(ProducerParameters.MessageRoutingMode.MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION)
                .setProducerName(producerName)
                .setInitialSequenceId(UInt64Value.of(initialSequenceId))
                .setHashingScheme(ProducerParameters.HashingScheme.HASHING_SCHEME_MURMUR3_32HASH)
                .setSendTimeoutMillis(UInt32Value.of(sendTimeoutMillis))
                .setBatchingEnabled(BoolValue.of(true))
                .setBatchingMaxMessages(UInt32Value.of(batchingMaxMessages))
                .setMaxPendingMessages(UInt32Value.of(maxPendingMessages))
                .setBatchingMaxPublishDelay(UInt64Value.of(batchingMaxPublishDelay))
                .setCompressionType(ProducerParameters.CompressionType.COMPRESSION_TYPE_LZ4)
                .build()
        )
        .build();
    Context.current().withValue(CLIENT_PARAMS_CTX_KEY, clientParams).attach();

    CountDownLatch latch = new CountDownLatch(1);

    PulsarClient pulsarClient = mock(PulsarClient.class);
    when(service.getPulsarClient()).thenReturn(pulsarClient);

    ProducerBuilderImpl producerBuilder = spy(new ProducerBuilderImpl(null, null));
    when(pulsarClient.newProducer()).thenReturn(producerBuilder);

    ProducerBase producer = mock(ProducerBase.class);
    doReturn(producer).when(producerBuilder).create();

//    TypedMessageBuilder message = mock(TypedMessageBuilder.class);
    TypedMessageBuilderImpl message = spy(new TypedMessageBuilderImpl(producer, Schema.BYTES));
    when(producer.newMessage()).thenReturn(message);

    MessageId messageId = DefaultImplementation.newMessageId(1, 2, 3);
    doReturn(CompletableFuture.completedFuture(messageId)).when(message).sendAsync();

    AtomicReference<ProducerAck> ackRef = new AtomicReference<>();

    StreamObserver<ProducerResponse> response = new StreamObserver<ProducerResponse>() {
      @Override
      public void onNext(ProducerResponse producerResponse) {
        if (producerResponse.hasAck()) {
          ackRef.set(producerResponse.getAck());
          latch.countDown();
        }
      }

      @Override
      public void onError(Throwable throwable) {

      }

      @Override
      public void onCompleted() {

      }
    };
    StreamObserver<ProducerRequest> requests = grpcService.produce(response);

    //TODO: test deliverAfter
    ProducerRequest request = ProducerRequest.newBuilder()
        .setSend(
            ProducerSend.newBuilder()
                .setPayload(ByteString.copyFromUtf8(payload))
                .setContext(context)
                .putProperties(propertyKey, propertyValue)
                .setKey(key)
                .addReplicationClusters(replicationCluster)
                .setEventTime(eventTime)
                .setDeliverAt(deliverAt)
        )
        .build();
    requests.onNext(request);

    assertTrue(latch.await(5, TimeUnit.SECONDS));

    // Check Ack
    ProducerAck ack = ackRef.get();
    assertEquals(context, ack.getContext());
    assertArrayEquals(messageId.toByteArray(), ack.getMessageId().toByteArray());

    // Verify producer default overridable values
    verify(producerBuilder, times(1)).enableBatching(false);
    verify(producerBuilder, times(1)).messageRoutingMode(MessageRoutingMode.SinglePartition);

    // Verify producer conf
    ProducerConfigurationData producerBuilderConf = producerBuilder.getConf();
    assertFalse(producerBuilderConf.isBlockIfQueueFull());
    assertEquals(producerName, producerBuilderConf.getProducerName());
    assertEquals(initialSequenceId, producerBuilderConf.getInitialSequenceId());
    assertEquals(HashingScheme.Murmur3_32Hash, producerBuilderConf.getHashingScheme());
    assertEquals(sendTimeoutMillis, producerBuilderConf.getSendTimeoutMs());
    assertTrue(producerBuilderConf.isBatchingEnabled());
    assertEquals(batchingMaxMessages, producerBuilderConf.getBatchingMaxMessages());
    assertEquals(maxPendingMessages, producerBuilderConf.getMaxPendingMessages());
    assertEquals(batchingMaxPublishDelay *1000, producerBuilderConf.getBatchingMaxPublishDelayMicros());
    assertEquals(MessageRoutingMode.RoundRobinPartition, producerBuilderConf.getMessageRoutingMode());
    assertEquals(CompressionType.LZ4, producerBuilderConf.getCompressionType());
    assertEquals("persistent://public/default/test-topic", producerBuilderConf.getTopicName());

    // Verify message properties
    MessageImpl sentMessage = (MessageImpl)message.getMessage();
    assertEquals(new String(message.getContent().array(), StandardCharsets.UTF_8), payload);
    assertEquals(propertyValue, sentMessage.getProperty(propertyKey));
    assertEquals(key, sentMessage.getKey());
    assertIterableEquals(Collections.singletonList(replicationCluster), sentMessage.getReplicateTo());
    assertEquals(eventTime, sentMessage.getEventTime());
    assertEquals(deliverAt, message.getMetadataBuilder().getDeliverAtTime());
  }
}
