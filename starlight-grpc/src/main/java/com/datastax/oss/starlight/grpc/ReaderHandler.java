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

import static com.datastax.oss.starlight.grpc.Converters.toConsumerCryptoFailureAction;
import static com.datastax.oss.starlight.grpc.Converters.toMessageId;
import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.oss.starlight.grpc.proto.ConsumerMessage;
import com.datastax.oss.starlight.grpc.proto.ReaderParameters;
import com.datastax.oss.starlight.grpc.proto.ReaderRequest;
import com.datastax.oss.starlight.grpc.proto.ReaderResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.MultiTopicsReaderImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderHandler extends AbstractGrpcHandler {

  private static final Logger log = LoggerFactory.getLogger(ReaderHandler.class);

  private final StreamObserver<ReaderResponse> messageStreamObserver;
  private final String subscription;
  private final Reader<byte[]> reader;
  private final AtomicInteger permits = new AtomicInteger();

  public ReaderHandler(
      GatewayService service, StreamObserver<ReaderResponse> messageStreamObserver) {
    super(service);
    this.messageStreamObserver = messageStreamObserver;
    ReaderParameters parameters = clientParameters.getReaderParameters();

    try {
      checkAuth();
      this.reader =
          getReaderConfiguration(parameters, service.getPulsarClient())
              .topic(topic.toString())
              .create();

      messageStreamObserver.onNext(
          ReaderResponse.newBuilder().setReaderSuccess(Empty.getDefaultInstance()).build());
      if (reader instanceof MultiTopicsReaderImpl) {
        this.subscription =
            ((MultiTopicsReaderImpl<?>) reader).getMultiTopicsConsumer().getSubscription();
      } else if (reader instanceof ReaderImpl) {
        this.subscription = ((ReaderImpl<?>) reader).getConsumer().getSubscription();
      } else {
        throw new IllegalArgumentException(
            String.format("Illegal Reader Type %s", reader.getClass()));
      }
    } catch (Exception e) {
      throw getStatus(e).withDescription(getErrorMessage(e)).asRuntimeException();
    }
  }

  private static String getErrorMessage(Exception e) {
    if (e instanceof IllegalArgumentException) {
      return "Invalid header params: " + e.getMessage();
    } else {
      return "Failed to create reader: " + e.getMessage();
    }
  }

  public StreamObserver<ReaderRequest> read() {
    // receiveMessage();
    return new StreamObserver<ReaderRequest>() {
      @Override
      public void onNext(ReaderRequest readerRequest) {
        if (readerRequest.hasPermits()) {
          if (permits.getAndAdd(readerRequest.getPermits().getPermits()) == 0) {
            // Resume delivery
            receiveMessage();
          }
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

  private void receiveMessage() {
    if (log.isDebugEnabled()) {
      log.debug("[{}] [{}] [{}] Receive next message", remoteAddress, topic, subscription);
    }

    reader
        .readNextAsync()
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
              if (msg.hasKey()) {
                dm.setKey(StringValue.of(msg.getKey()));
              }

              synchronized (messageStreamObserver) {
                messageStreamObserver.onNext(ReaderResponse.newBuilder().setMessage(dm).build());
              }

              if (permits.decrementAndGet() > 0) {
                // Start next read in a separate thread to avoid recursion
                service.getExecutor().execute(this::receiveMessage);
              }
            })
        .exceptionally(
            exception -> {
              if (exception.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                log.info(
                    "[{}/{}] Reader was closed while receiving msg from broker",
                    reader.getTopic(),
                    subscription);
              } else {
                log.warn(
                    "[{}/{}] Error occurred while reader handler was delivering msg to {}: {}",
                    reader.getTopic(),
                    subscription,
                    remoteAddress,
                    exception.getMessage());
              }
              return null;
            });
  }

  @Override
  public void close() {
    if (reader != null) {
      reader
          .closeAsync()
          .thenAccept(
              x -> {
                if (log.isDebugEnabled()) {
                  log.debug("[{}] Closed reader asynchronously", reader.getTopic());
                }
              })
          .exceptionally(
              exception -> {
                log.warn("[{}] Failed to close reader", reader.getTopic(), exception);
                return null;
              });
    }
  }

  private ReaderBuilder<byte[]> getReaderConfiguration(ReaderParameters params, PulsarClient client)
      throws IOException {

    ReaderBuilder<byte[]> builder = client.newReader();

    if (!params.getReaderName().isEmpty()) {
      builder.readerName(params.getReaderName());
    }

    if (params.hasReceiverQueueSize()) {
      int receiverQueueSize = params.getReceiverQueueSize().getValue();
      checkArgument(receiverQueueSize >= 0, "receiverQueueSize needs to be >= 0");
      // TODO: make max receiver queue size a proxy conf param
      builder.receiverQueueSize(Math.min(receiverQueueSize, 1000));
    }

    ConsumerCryptoFailureAction action =
        toConsumerCryptoFailureAction(params.getCryptoFailureAction());
    if (action != null) {
      builder.cryptoFailureAction(action);
    }

    switch (params.getMessageIdOneofCase()) {
      case MESSAGE_ID:
        builder.startMessageId(MessageId.fromByteArray(params.getMessageId().toByteArray()));
        break;
      case MESSAGE_ID_MODE:
        builder.startMessageId(toMessageId(params.getMessageIdMode()));
        break;
      default:
        builder.startMessageId(MessageId.latest);
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
}
