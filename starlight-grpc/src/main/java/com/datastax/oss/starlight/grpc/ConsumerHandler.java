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

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.oss.starlight.grpc.proto.ConsumerAck;
import com.datastax.oss.starlight.grpc.proto.ConsumerEndOfTopicResponse;
import com.datastax.oss.starlight.grpc.proto.ConsumerMessage;
import com.datastax.oss.starlight.grpc.proto.ConsumerNack;
import com.datastax.oss.starlight.grpc.proto.ConsumerParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerRequest;
import com.datastax.oss.starlight.grpc.proto.ConsumerResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.DeadLetterPolicy.DeadLetterPolicyBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerHandler extends AbstractGrpcHandler {

  private static final Logger log = LoggerFactory.getLogger(ConsumerHandler.class);

  private final StreamObserver<ConsumerResponse> messageStreamObserver;
  private final String subscription;
  private final Consumer<byte[]> consumer;

  private final int maxPendingMessages;
  private final AtomicInteger pendingMessages = new AtomicInteger();

  // Make sure use the same BatchMessageIdImpl to acknowledge the batch message, otherwise the
  // BatchMessageAcker of the BatchMessageIdImpl will not complete.
  private final Cache<ByteString, MessageId> messageIdCache =
      CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build();

  ConsumerHandler(GatewayService service, StreamObserver<ConsumerResponse> messageStreamObserver) {
    super(service);
    this.messageStreamObserver = messageStreamObserver;
    ConsumerParameters parameters = clientParameters.getConsumerParameters();

    try {
      // checkAuth() and getConsumerConfiguration() should be called after assigning a value to
      // this.subscription
      this.subscription = parameters.getSubscription();
      checkArgument(!subscription.isEmpty(), "Empty subscription name");

      ConsumerBuilderImpl<byte[]> builder =
          (ConsumerBuilderImpl<byte[]>)
              getConsumerConfiguration(parameters, service.getPulsarClient());
      this.maxPendingMessages =
          (builder.getConf().getReceiverQueueSize() == 0)
              ? 1
              : builder.getConf().getReceiverQueueSize();

      checkAuth();

      consumer = builder.topic(topic.toString()).subscriptionName(subscription).subscribe();

    } catch (Exception e) {
      throw getStatus(e).withDescription(getErrorMessage(e)).asRuntimeException();
    }
  }

  public StreamObserver<ConsumerRequest> consume() {
    receiveMessage();
    return new StreamObserver<ConsumerRequest>() {
      @Override
      public void onNext(ConsumerRequest request) {
        try {
          switch (request.getRequestCase()) {
            case ACK:
              handleAck(request.getAck());
              break;
            case NACK:
              handleNack(request.getNack());
              break;
            case UNSUBSCRIBE:
              handleUnsubscribe();
              break;
            case END_OF_TOPIC:
              handleEndOfTopic();
              break;
            default:
              break;
          }
        } catch (Exception e) {
          synchronized (messageStreamObserver) {
            messageStreamObserver.onError(e);
          }
          close();
        }
      }

      @Override
      public void onError(Throwable t) {
        synchronized (messageStreamObserver) {
          messageStreamObserver.onError(t);
        }
        close();
      }

      @Override
      public void onCompleted() {
        synchronized (messageStreamObserver) {
          messageStreamObserver.onCompleted();
        }
        close();
      }
    };
  }

  // Check and notify consumer if reached end of topic.
  private void handleEndOfTopic() {
    if (log.isDebugEnabled()) {
      log.debug(
          "[{}/{}] Received check reach the end of topic request from {} ",
          consumer.getTopic(),
          subscription,
          remoteAddress);
    }
    synchronized (messageStreamObserver) {
      messageStreamObserver.onNext(
          ConsumerResponse.newBuilder()
              .setEndOfTopicResponse(
                  ConsumerEndOfTopicResponse.newBuilder()
                      .setReachedEndOfTopic(consumer.hasReachedEndOfTopic()))
              .build());
    }
  }

  private void handleUnsubscribe() {
    if (log.isDebugEnabled()) {
      log.debug(
          "[{}/{}] Received unsubscribe request from {} ",
          consumer.getTopic(),
          subscription,
          remoteAddress);
    }
    consumer.unsubscribeAsync();
  }

  private void checkResumeReceive() {
    int pending = pendingMessages.getAndDecrement();
    if (pending >= maxPendingMessages) {
      // Resume delivery
      receiveMessage();
    }
  }

  private void handleAck(ConsumerAck command) throws IOException {
    // We should have received an ack
    MessageId msgId =
        MessageId.fromByteArrayWithTopic(command.getMessageId().toByteArray(), topic.toString());
    if (log.isDebugEnabled()) {
      log.debug(
          "[{}/{}] Received ack request of message {} from {} ",
          consumer.getTopic(),
          subscription,
          msgId,
          remoteAddress);
    }

    MessageId originalMsgId = messageIdCache.asMap().remove(command.getMessageId());
    if (originalMsgId != null) {
      consumer.acknowledgeAsync(originalMsgId);
    } else {
      consumer.acknowledgeAsync(msgId);
    }

    checkResumeReceive();
  }

  private void handleNack(ConsumerNack command) throws IOException {
    MessageId msgId =
        MessageId.fromByteArrayWithTopic(command.getMessageId().toByteArray(), topic.toString());
    if (log.isDebugEnabled()) {
      log.debug(
          "[{}/{}] Received negative ack request of message {} from {} ",
          consumer.getTopic(),
          subscription,
          msgId,
          remoteAddress);
    }

    MessageId originalMsgId = messageIdCache.asMap().remove(command.getMessageId());
    if (originalMsgId != null) {
      consumer.negativeAcknowledge(originalMsgId);
    } else {
      consumer.negativeAcknowledge(msgId);
    }
    checkResumeReceive();
  }

  private static String getErrorMessage(Exception e) {
    if (e instanceof IllegalArgumentException) {
      return "Invalid header params: " + e.getMessage();
    } else {
      return "Failed to subscribe: " + e.getMessage();
    }
  }

  private void receiveMessage() {
    if (log.isDebugEnabled()) {
      log.debug("[{}] [{}] [{}] Receive next message", remoteAddress, topic, subscription);
    }

    consumer
        .receiveAsync()
        .thenAccept(
            msg -> {
              if (log.isDebugEnabled()) {
                log.debug(
                    "[{}] [{}] [{}] Got message {}",
                    remoteAddress,
                    topic,
                    subscription,
                    msg.getMessageId());
              }

              ConsumerMessage.Builder dm = ConsumerMessage.newBuilder();
              dm.setMessageId(ByteString.copyFrom(msg.getMessageId().toByteArray()));
              dm.setPayload(ByteString.copyFrom(msg.getData()));
              dm.putAllProperties(msg.getProperties());
              dm.setPublishTime(msg.getPublishTime());
              dm.setEventTime(msg.getEventTime());
              // TODO: needs proto hasKey or empty string is OK ?
              if (msg.hasKey()) {
                dm.setKey(msg.getKey());
              }
              // final long msgSize = msg.getData().length;
              messageIdCache.put(dm.getMessageId(), msg.getMessageId());

              synchronized (messageStreamObserver) {
                messageStreamObserver.onNext(ConsumerResponse.newBuilder().setMessage(dm).build());
              }

              int pending = pendingMessages.incrementAndGet();
              if (pending < maxPendingMessages) {
                // Start next read in a separate thread to avoid recursion
                service.getExecutor().execute(this::receiveMessage);
              }
            })
        .exceptionally(
            exception -> {
              if (exception.getCause() instanceof AlreadyClosedException) {
                log.info(
                    "[{}/{}] Consumer was closed while receiving msg from broker",
                    consumer.getTopic(),
                    subscription);
              } else {
                log.warn(
                    "[{}/{}] Error occurred while consumer handler was delivering msg to {}: {}",
                    consumer.getTopic(),
                    subscription,
                    remoteAddress,
                    exception.getMessage());
              }
              synchronized (messageStreamObserver) {
                messageStreamObserver.onError(exception);
              }
              close();
              return null;
            });
  }

  private static SubscriptionType toSubscriptionType(ConsumerParameters.SubscriptionType type) {
    switch (type) {
      case SUBSCRIPTION_TYPE_EXCLUSIVE:
        return SubscriptionType.Exclusive;
      case SUBSCRIPTION_TYPE_FAILOVER:
        return SubscriptionType.Failover;
      case SUBSCRIPTION_TYPE_SHARED:
        return SubscriptionType.Shared;
      case SUBSCRIPTION_TYPE_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid subscription type");
    }
  }

  private static ConsumerCryptoFailureAction toConsumerCryptoFailureAction(
      ConsumerParameters.ConsumerCryptoFailureAction action) {
    switch (action) {
      case CONSUMER_CRYPTO_FAILURE_ACTION_FAIL:
        return ConsumerCryptoFailureAction.FAIL;
      case CONSUMER_CRYPTO_FAILURE_ACTION_DISCARD:
        return ConsumerCryptoFailureAction.DISCARD;
      case CONSUMER_CRYPTO_FAILURE_ACTION_CONSUME:
        return ConsumerCryptoFailureAction.CONSUME;
      case CONSUMER_CRYPTO_FAILURE_ACTION_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid consumer crypto failure action");
    }
  }

  private ConsumerBuilder<byte[]> getConsumerConfiguration(
      ConsumerParameters params, PulsarClient client) {
    ConsumerBuilder<byte[]> builder = client.newConsumer();

    if (params.hasAckTimeoutMillis()) {
      builder.ackTimeout(params.getAckTimeoutMillis().getValue(), TimeUnit.MILLISECONDS);
    }

    SubscriptionType value = toSubscriptionType(params.getSubscriptionType());
    if (value != null) {
      builder.subscriptionType(value);
    }

    if (params.hasReceiverQueueSize()) {
      builder.receiverQueueSize(Math.min(params.getReceiverQueueSize().getValue(), 1000));
    }

    if (!params.getConsumerName().isEmpty()) {
      builder.consumerName(params.getConsumerName());
    }

    if (params.hasPriorityLevel()) {
      builder.priorityLevel(params.getPriorityLevel().getValue());
    }

    if (params.hasNegativeAckRedeliveryDelayMillis()) {
      builder.negativeAckRedeliveryDelay(
          params.getNegativeAckRedeliveryDelayMillis().getValue(), TimeUnit.MILLISECONDS);
    }

    if (params.hasDeadLetterPolicy()) {
      com.datastax.oss.starlight.grpc.proto.DeadLetterPolicy deadLetterPolicy =
          params.getDeadLetterPolicy();
      DeadLetterPolicyBuilder dlpBuilder = DeadLetterPolicy.builder();
      if (deadLetterPolicy.hasMaxRedeliverCount()) {
        dlpBuilder
            .maxRedeliverCount(deadLetterPolicy.getMaxRedeliverCount().getValue())
            .deadLetterTopic(String.format("%s-%s-DLQ", topic, subscription));
      }

      if (!deadLetterPolicy.getDeadLetterTopic().isEmpty()) {
        dlpBuilder.deadLetterTopic(deadLetterPolicy.getDeadLetterTopic());
      }
      builder.deadLetterPolicy(dlpBuilder.build());
    }

    ConsumerCryptoFailureAction action =
        toConsumerCryptoFailureAction(params.getCryptoFailureAction());
    if (action != null) {
      builder.cryptoFailureAction(action);
    }
    return builder;
  }

  @Override
  protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData)
      throws Exception {
    return service
        .getAuthorizationService()
        .canConsume(topic, authRole, authenticationData, this.subscription);
  }

  @Override
  public void close() {
    if (consumer != null) {
      consumer
          .closeAsync()
          .thenAccept(
              x -> {
                if (log.isDebugEnabled()) {
                  log.debug("[{}] Closed consumer asynchronously", consumer.getTopic());
                }
              })
          .exceptionally(
              exception -> {
                log.warn("[{}] Failed to close consumer", consumer.getTopic(), exception);
                return null;
              });
    }
  }

  @VisibleForTesting
  Cache<ByteString, MessageId> getMessageIdCache() {
    return messageIdCache;
  }
}
