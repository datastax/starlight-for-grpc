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
import static com.datastax.oss.starlight.grpc.Constants.AUTHORIZATION_METADATA_KEY;
import static com.datastax.oss.starlight.grpc.Constants.AUTH_METHOD_METADATA_KEY;

import io.grpc.*;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationServerInterceptor implements ServerInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationServerInterceptor.class);

  private final AuthenticationService authenticationService;

  public AuthenticationServerInterceptor(AuthenticationService authenticationService) {
    this.authenticationService = authenticationService;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata metadata,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {
    Context ctx = Context.current();

    SocketAddress socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    try {
      Map<String, String> authHeaders = new HashMap<>();
      if (metadata.containsKey(AUTHORIZATION_METADATA_KEY)) {
        authHeaders.put("Authorization", metadata.get(AUTHORIZATION_METADATA_KEY));
      }
      SSLSession sslSession = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
      AuthenticationDataSource authData =
          new AuthenticationDataGrpc(socketAddress, sslSession, authHeaders);
      String authMethodName = metadata.get(AUTH_METHOD_METADATA_KEY);
      String role = authenticateData(authData, authMethodName);
      ctx =
          ctx.withValue(AUTHENTICATION_ROLE_CTX_KEY, role)
              .withValue(AUTHENTICATION_DATA_CTX_KEY, authData);
      if (LOG.isDebugEnabled()) {
        LOG.debug("[{}] Authenticated HTTP request with role {}", socketAddress, role);
      }
    } catch (AuthenticationException e) {
      LOG.warn("[{}] Failed to authenticate HTTP request: {}", socketAddress, e.getMessage());
      throw Status.UNAUTHENTICATED.withDescription(e.getMessage()).asRuntimeException();
    }

    return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
  }

  private String authenticateData(AuthenticationDataSource authData, String authMethodName)
      throws AuthenticationException {
    if (authMethodName != null) {
      AuthenticationProvider providerToUse =
          authenticationService.getAuthenticationProvider(authMethodName);
      if (providerToUse == null) {
        throw new AuthenticationException(
            String.format("Unsupported authentication method: [%s].", authMethodName));
      } else {
        try {
          return providerToUse.authenticate(authData);
        } catch (AuthenticationException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Authentication failed for provider "
                    + providerToUse.getAuthMethodName()
                    + ": "
                    + e.getMessage(),
                e);
          }
          throw e;
        }
      }
    } else {
      for (String authMethodName2 : Arrays.asList("token", "basic", "tls")) {
        try {
          return authenticationService.authenticate(authData, authMethodName2);
        } catch (AuthenticationException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Authentication failed for provider " + authMethodName2 + ": " + e.getMessage(), e);
          }
        }
      }
      return authenticationService
          .getAnonymousUserRole()
          .orElseThrow(() -> new AuthenticationException("Authentication required"));
    }
  }
}
