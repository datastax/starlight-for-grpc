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

import static com.datastax.oss.starlight.grpc.Constants.AUTHENTICATION_DATA_CTX_KEY;
import static com.datastax.oss.starlight.grpc.Constants.AUTHENTICATION_ROLE_CTX_KEY;
import static com.datastax.oss.starlight.grpc.Constants.CLIENT_PARAMS_CTX_KEY;
import static com.datastax.oss.starlight.grpc.Constants.REMOTE_ADDRESS_CTX_KEY;

import com.datastax.oss.starlight.grpc.proto.ClientParameters;
import io.grpc.Status;
import java.io.Closeable;
import java.net.InetSocketAddress;
import javax.naming.NoPermissionException;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGrpcHandler implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(AbstractGrpcHandler.class);

  protected final GatewayService service;
  protected final TopicName topic;
  protected final ClientParameters clientParameters;
  protected final InetSocketAddress remoteAddress;
  protected final String authenticationRole;
  protected final AuthenticationDataSource authenticationData;

  public AbstractGrpcHandler(GatewayService service) {
    this.service = service;
    this.topic = TopicName.get(CLIENT_PARAMS_CTX_KEY.get().getTopic());
    this.clientParameters = CLIENT_PARAMS_CTX_KEY.get();
    this.remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
    this.authenticationRole = AUTHENTICATION_ROLE_CTX_KEY.get();
    this.authenticationData = AUTHENTICATION_DATA_CTX_KEY.get();
  }

  protected void checkAuth() throws Exception {
    if (service.isAuthorizationEnabled()) {
      Boolean authorized;
      try {
        authorized = isAuthorized(authenticationRole, authenticationData);
      } catch (Exception e) {
        log.warn(
            "[{}] Got an exception when authorizing gRPC client {} on topic {} on: {}",
            remoteAddress,
            authenticationRole,
            topic,
            e.getMessage());
        throw e;
      }
      if (!authorized) {
        log.warn(
            "[{}] gRPC Client [{}] is not authorized on topic {}",
            remoteAddress,
            authenticationRole,
            topic.toString());
        throw new NoPermissionException("Not authorized");
      }
    }
  }

  protected static Status getStatus(Exception e) {
    if (e instanceof IllegalArgumentException) {
      return Status.INVALID_ARGUMENT;
    } else if (e instanceof PulsarClientException.AuthenticationException) {
      return Status.UNAUTHENTICATED;
    } else if (e instanceof PulsarClientException.AuthorizationException
        || e instanceof NoPermissionException) {
      return Status.PERMISSION_DENIED;
    } else if (e instanceof PulsarClientException.NotFoundException
        || e instanceof PulsarClientException.TopicDoesNotExistException) {
      return Status.NOT_FOUND;
    } else if (e instanceof PulsarClientException.ProducerBusyException
        || e instanceof PulsarClientException.ConsumerBusyException
        || e instanceof PulsarClientException.ProducerFencedException
        || e instanceof PulsarClientException.IncompatibleSchemaException) {
      return Status.FAILED_PRECONDITION;
    } else if (e instanceof PulsarClientException.TooManyRequestsException) {
      return Status.UNAVAILABLE; // Too Many Requests
    } else if (e instanceof PulsarClientException.ProducerBlockedQuotaExceededError
        || e instanceof PulsarClientException.ProducerBlockedQuotaExceededException
        || e instanceof PulsarClientException.TopicTerminatedException) {
      return Status.UNAVAILABLE;
    } else if (e instanceof PulsarClientException.TimeoutException) {
      return Status.ABORTED;
    } else {
      return Status.INTERNAL;
    }
  }

  protected abstract Boolean isAuthorized(
      String authRole, AuthenticationDataSource authenticationData) throws Exception;
}
