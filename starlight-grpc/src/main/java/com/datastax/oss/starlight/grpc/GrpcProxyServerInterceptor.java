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
import static com.datastax.oss.starlight.grpc.Constants.CLIENT_PARAMS_METADATA_KEY;
import static com.datastax.oss.starlight.grpc.Constants.REMOTE_ADDRESS_CTX_KEY;
import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class GrpcProxyServerInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata metadata,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {
    Context ctx = Context.current();

    if (metadata.containsKey(CLIENT_PARAMS_METADATA_KEY)) {
      try {
        ClientParameters params =
            ClientParameters.parseFrom(metadata.get(CLIENT_PARAMS_METADATA_KEY));
        checkArgument(!params.getTopic().isEmpty(), "Empty topic name");
        ctx = ctx.withValue(CLIENT_PARAMS_CTX_KEY, params);
      } catch (InvalidProtocolBufferException e) {
        serverCall.close(Status.INVALID_ARGUMENT.withDescription(e.getMessage()), new Metadata());
        return new ServerCall.Listener<ReqT>() {};
      }
    }

    SocketAddress socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (socketAddress instanceof InetSocketAddress) {
      ctx = ctx.withValue(REMOTE_ADDRESS_CTX_KEY, (InetSocketAddress) socketAddress);
    }

    return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
  }
}
