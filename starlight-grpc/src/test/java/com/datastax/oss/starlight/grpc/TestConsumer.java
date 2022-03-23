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
import static com.datastax.oss.starlight.grpc.proto.ConsumerParameters.SubscriptionType.SUBSCRIPTION_TYPE_SHARED;

import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerAck;
import com.datastax.oss.starlight.grpc.proto.ConsumerMessage;
import com.datastax.oss.starlight.grpc.proto.ConsumerParameters;
import com.datastax.oss.starlight.grpc.proto.ConsumerRequest;
import com.datastax.oss.starlight.grpc.proto.ConsumerResponse;
import com.datastax.oss.starlight.grpc.proto.DeadLetterPolicy;
import com.datastax.oss.starlight.grpc.proto.PulsarGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.LoggerFactory;

public class TestConsumer {

  public static void main(String[] args) throws Exception {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();

    Metadata headers = new Metadata();
    ClientParameters params =
        ClientParameters.newBuilder()
            .setTopic("test-topic")
            .setConsumerParameters(
                ConsumerParameters.newBuilder()
                    .setSubscription("my-subscription")
                    .setAckTimeoutMillis(uint64Value(2000))
                    .setSubscriptionType(SUBSCRIPTION_TYPE_SHARED)
                    .setDeadLetterPolicy(
                        DeadLetterPolicy.newBuilder().setMaxRedeliverCount(uint32Value(3))))
            .build();
    headers.put(CLIENT_PARAMS_METADATA_KEY, params.toByteArray());
    PulsarGrpc.PulsarStub asyncStub =
        MetadataUtils.attachHeaders(PulsarGrpc.newStub(channel), headers);

    LinkedBlockingQueue<byte[]> messagesToack = new LinkedBlockingQueue<>(1000);

    StreamObserver<ConsumerResponse> messageStreamObserver =
        new StreamObserver<ConsumerResponse>() {
          @Override
          public void onNext(ConsumerResponse response) {
            if (response.hasMessage()) {
              ConsumerMessage value = response.getMessage();
              byte[] msgId = value.getMessageId().toByteArray();
              try {
                messagesToack.put(msgId);
              } catch (InterruptedException e) {
                onError(e);
              }
              String payload = value.getPayload().toStringUtf8();
              System.out.println("consumer received: " + payload + " ");
            }
          }

          @Override
          public void onError(Throwable t) {
            System.out.println("consumer message error: " + t.getMessage());
            LoggerFactory.getLogger("foo").error(t.getMessage());
          }

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ConsumerRequest> ackStreamObserver = asyncStub.consume(messageStreamObserver);

    while (true) {
      byte[] msgId = messagesToack.poll();
      if (msgId != null) {
        ackStreamObserver.onNext(
            ConsumerRequest.newBuilder()
                .setAck(ConsumerAck.newBuilder().setMessageId(ByteString.copyFrom(msgId)))
                .build());
      }
      Thread.sleep(1);
    }
  }

  public static UInt64Value uint64Value(long value) {
    return UInt64Value.newBuilder().setValue(value).build();
  }

  public static UInt32Value uint32Value(int value) {
    return UInt32Value.newBuilder().setValue(value).build();
  }
}
