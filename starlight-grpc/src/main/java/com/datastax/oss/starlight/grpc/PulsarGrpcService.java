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

import com.datastax.oss.starlight.grpc.proto.ConsumerRequest;
import com.datastax.oss.starlight.grpc.proto.ConsumerResponse;
import com.datastax.oss.starlight.grpc.proto.ProducerRequest;
import com.datastax.oss.starlight.grpc.proto.ProducerResponse;
import com.datastax.oss.starlight.grpc.proto.PulsarGrpc;
import com.google.protobuf.StringValue;
import io.grpc.stub.StreamObserver;

public class PulsarGrpcService extends PulsarGrpc.PulsarImplBase {

  private final GatewayService service;

  public PulsarGrpcService(GatewayService service) {
    this.service = service;
  }

  @Override
  public StreamObserver<ProducerRequest> produce(StreamObserver<ProducerResponse> streamObserver) {
    ProducerHandler handler = new ProducerHandler(service, streamObserver);
    return handler.produce();
  }

  @Override
  public StreamObserver<ConsumerRequest> consume(StreamObserver<ConsumerResponse> streamObserver) {
    ConsumerHandler handler = new ConsumerHandler(service, streamObserver);
    return handler.consume();
  }

  @Override
  public void ping(StringValue request, StreamObserver<StringValue> responseObserver) {
    responseObserver.onNext(request);
    responseObserver.onCompleted();
  }
}
