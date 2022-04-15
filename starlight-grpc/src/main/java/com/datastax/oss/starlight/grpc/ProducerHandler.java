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

import com.datastax.oss.starlight.grpc.proto.ProducerAck;
import com.datastax.oss.starlight.grpc.proto.ProducerParameters;
import com.datastax.oss.starlight.grpc.proto.ProducerRequest;
import com.datastax.oss.starlight.grpc.proto.ProducerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerSend;
import com.datastax.oss.starlight.grpc.proto.ProducerSendError;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerHandler extends AbstractGrpcHandler {

  private static final Logger log = LoggerFactory.getLogger(ProducerHandler.class);

  private final StreamObserver<ProducerResponse> responseStreamObserver;
  private final Producer<byte[]> producer;

  public ProducerHandler(
      GatewayService service, StreamObserver<ProducerResponse> responseStreamObserver) {
    super(service);
    this.responseStreamObserver = responseStreamObserver;
    ProducerParameters parameters = clientParameters.getProducerParameters();

    try {
      checkAuth();
      this.producer =
          getProducerBuilder(parameters, service.getPulsarClient())
              .topic(topic.toString())
              .create();

      responseStreamObserver.onNext(
          ProducerResponse.newBuilder().setProducerSuccess(Empty.getDefaultInstance()).build());
    } catch (Exception e) {
      log.warn(
          "[{}] Failed in creating producer on topic {}: {}", remoteAddress, topic, e.getMessage());
      throw getStatus(e).withDescription(getErrorMessage(e)).asRuntimeException();
    }
  }

  private static String getErrorMessage(Exception e) {
    if (e instanceof IllegalArgumentException) {
      return "Invalid header params: " + e.getMessage();
    } else {
      return "Failed to create producer: " + e.getMessage();
    }
  }

  public StreamObserver<ProducerRequest> produce() {
    return new StreamObserver<ProducerRequest>() {
      @Override
      public void onNext(ProducerRequest request) {
        if (request.getRequestCase() == ProducerRequest.RequestCase.SEND) {
          ProducerSend message = request.getSend();
          TypedMessageBuilder<byte[]> builder = producer.newMessage();
          try {
            builder.value(message.getPayload().toByteArray());
          } catch (SchemaSerializationException e) {
            synchronized (responseStreamObserver) {
              responseStreamObserver.onNext(
                  ProducerResponse.newBuilder()
                      .setContext(request.getContext())
                      .setError(
                          ProducerSendError.newBuilder()
                              .setStatusCode(Code.INVALID_ARGUMENT.value())
                              .setErrorMsg(e.getMessage()))
                      .build());
            }
            return;
          }

          if (message.getPropertiesCount() != 0) {
            builder.properties(message.getPropertiesMap());
          }
          if (!Strings.isNullOrEmpty(message.getKey())) {
            builder.key(message.getKey());
          }
          if (message.getReplicationClustersCount() != 0) {
            builder.replicationClusters(message.getReplicationClustersList());
          }
          if (message.getEventTime() > 0) {
            builder.eventTime(message.getEventTime());
          }
          if (message.getDeliverAt() > 0) {
            builder.deliverAt(message.getDeliverAt());
          }
          if (message.getDeliverAfterMs() > 0) {
            builder.deliverAfter(message.getDeliverAfterMs(), TimeUnit.MILLISECONDS);
          }

          // final long now = System.nanoTime();
          builder
              .sendAsync()
              .thenAccept(
                  msgId -> {
                    // updateSentMsgStats(msgSize, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() -
                    // now));
                    synchronized (responseStreamObserver) {
                      responseStreamObserver.onNext(
                          ProducerResponse.newBuilder()
                              .setContext(request.getContext())
                              .setAck(
                                  ProducerAck.newBuilder()
                                      .setMessageId(ByteString.copyFrom(msgId.toByteArray())))
                              .build());
                    }
                  })
              .exceptionally(
                  exception -> {
                    log.warn(
                        "[{}] Error occurred while producer handler was sending msg from {}: {}",
                        producer.getTopic(),
                        remoteAddress,
                        exception.getMessage());
                    // numMsgsFailed.increment();
                    synchronized (responseStreamObserver) {
                      responseStreamObserver.onNext(
                          ProducerResponse.newBuilder()
                              .setContext(request.getContext())
                              .setError(
                                  ProducerSendError.newBuilder()
                                      .setStatusCode(Code.INTERNAL.value())
                                      .setErrorMsg(exception.getMessage()))
                              .build());
                    }
                    return null;
                  });
        }
      }

      @Override
      public void onError(Throwable t) {
        synchronized (responseStreamObserver) {
          responseStreamObserver.onError(t);
        }
        close();
      }

      @Override
      public void onCompleted() {
        synchronized (responseStreamObserver) {
          responseStreamObserver.onCompleted();
        }
        close();
      }
    };
  }

  @Override
  public void close() {
    if (producer != null) {
      producer
          .closeAsync()
          .thenAccept(
              x -> {
                if (log.isDebugEnabled()) {
                  log.debug("[{}] Closed producer asynchronously", producer.getTopic());
                }
              })
          .exceptionally(
              exception -> {
                log.warn("[{}] Failed to close producer", producer.getTopic(), exception);
                return null;
              });
    }
  }

  private static HashingScheme toHashingScheme(ProducerParameters.HashingScheme scheme) {
    switch (scheme) {
      case HASHING_SCHEME_JAVA_STRING_HASH:
        return HashingScheme.JavaStringHash;
      case HASHING_SCHEME_MURMUR3_32HASH:
        return HashingScheme.Murmur3_32Hash;
      case HASHING_SCHEME_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid hashing scheme");
    }
  }

  private static MessageRoutingMode toMessageRoutingMode(
      ProducerParameters.MessageRoutingMode mode) {
    switch (mode) {
      case MESSAGE_ROUTING_MODE_SINGLE_PARTITION:
        return MessageRoutingMode.SinglePartition;
      case MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION:
        return MessageRoutingMode.RoundRobinPartition;
      case MESSAGE_ROUTING_MODE_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid message routing mode");
    }
  }

  private static CompressionType toCompressionType(ProducerParameters.CompressionType type) {
    switch (type) {
      case COMPRESSION_TYPE_NONE:
        return CompressionType.NONE;
      case COMPRESSION_TYPE_LZ4:
        return CompressionType.LZ4;
      case COMPRESSION_TYPE_ZLIB:
        return CompressionType.ZLIB;
      case COMPRESSION_TYPE_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid compression type");
    }
  }

  private ProducerBuilder<byte[]> getProducerBuilder(
      ProducerParameters params, PulsarClient client) {
    ProducerBuilder<byte[]> builder =
        client
            .newProducer()
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition);

    // Set to false to prevent the server thread from being blocked if a lot of messages are
    // pending.
    builder.blockIfQueueFull(false);

    if (!params.getProducerName().isEmpty()) {
      builder.producerName(params.getProducerName());
    }

    if (params.hasInitialSequenceId()) {
      builder.initialSequenceId(params.getInitialSequenceId().getValue());
    }

    HashingScheme hashingScheme = toHashingScheme(params.getHashingScheme());
    if (hashingScheme != null) {
      builder.hashingScheme(hashingScheme);
    }

    if (params.hasSendTimeoutMillis()) {
      builder.sendTimeout(params.getSendTimeoutMillis().getValue(), TimeUnit.MILLISECONDS);
    }

    if (params.hasBatchingEnabled()) {
      builder.enableBatching(params.getBatchingEnabled().getValue());
    }

    if (params.hasBatchingMaxMessages()) {
      builder.batchingMaxMessages(params.getBatchingMaxMessages().getValue());
    }

    if (params.hasMaxPendingMessages()) {
      builder.maxPendingMessages(params.getMaxPendingMessages().getValue());
    }

    if (params.hasBatchingMaxPublishDelayMillis()) {
      builder.batchingMaxPublishDelay(
          params.getBatchingMaxPublishDelayMillis().getValue(), TimeUnit.MILLISECONDS);
    }

    MessageRoutingMode messageRoutingMode = toMessageRoutingMode(params.getMessageRoutingMode());
    if (messageRoutingMode != null) {
      builder.messageRoutingMode(messageRoutingMode);
    }

    CompressionType compressionType = toCompressionType(params.getCompressionType());
    if (compressionType != null) {
      builder.compressionType(compressionType);
    }

    return builder;
  }

  @Override
  protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData)
      throws Exception {
    return service.getAuthorizationService().canProduce(topic, authRole, authenticationData);
  }
}
