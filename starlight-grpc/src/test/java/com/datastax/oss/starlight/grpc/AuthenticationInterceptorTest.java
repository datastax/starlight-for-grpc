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

import static com.datastax.oss.starlight.grpc.Constants.AUTHENTICATION_ROLE_CTX_KEY;
import static com.datastax.oss.starlight.grpc.Constants.AUTHORIZATION_METADATA_KEY;
import static com.datastax.oss.starlight.grpc.Constants.AUTH_METHOD_METADATA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.datastax.oss.starlight.grpc.proto.PulsarGrpc;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderBasic;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link AuthenticationInterceptor}. */
public class AuthenticationInterceptorTest {
  private static final String localHostname = "localhost";

  private static final String TLS_TRUST_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/cacert.pem";
  private static final String TLS_SERVER_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/server-cert.pem";
  private static final String TLS_SERVER_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/server-key.pem";
  private static final String TLS_CLIENT_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/client-cert.pem";
  private static final String TLS_CLIENT_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/client-key.pem";
  private static final String BASIC_CONF_FILE_PATH =
      "./src/test/resources/authentication/basic/.htpasswd";

  public static final StringValue PING = StringValue.of("ping");

  private AuthenticationService authenticationService;
  private Server server;
  private ManagedChannel channel;
  private PulsarGrpc.PulsarBlockingStub stub;
  private ContextInterceptor contextInterceptor;

  @BeforeEach
  public void setup() throws Exception {
    System.setProperty("pulsar.auth.basic.conf", BASIC_CONF_FILE_PATH);
    authenticationService = mock(AuthenticationService.class);

    SslContext sslContext =
        GrpcSslContexts.forServer(
                new File(TLS_SERVER_CERT_FILE_PATH), new File(TLS_SERVER_KEY_FILE_PATH))
            .trustManager(new File(TLS_TRUST_CERT_FILE_PATH))
            .clientAuth(ClientAuth.REQUIRE)
            .build();

    contextInterceptor = new ContextInterceptor();

    AuthenticationInterceptor authenticationInterceptor =
        new AuthenticationInterceptor(authenticationService);

    server =
        NettyServerBuilder.forAddress(new InetSocketAddress(localHostname, 0))
            .sslContext(sslContext)
            .addService(
                ServerInterceptors.intercept(
                    new PulsarGrpcService(null),
                    Arrays.asList(contextInterceptor, authenticationInterceptor)))
            .build();

    server.start();

    SslContext clientSslContext =
        GrpcSslContexts.forClient()
            .trustManager(new File(TLS_TRUST_CERT_FILE_PATH))
            .keyManager(new File(TLS_CLIENT_CERT_FILE_PATH), new File(TLS_CLIENT_KEY_FILE_PATH))
            .build();

    channel =
        NettyChannelBuilder.forAddress(new InetSocketAddress(localHostname, server.getPort()))
            .usePlaintext()
            .negotiationType(NegotiationType.TLS)
            .sslContext(clientSslContext)
            .build();

    stub = PulsarGrpc.newBlockingStub(channel);
  }

  @AfterEach
  public void teardown() throws InterruptedException {
    server.shutdownNow();
    channel.shutdownNow();
    server.awaitTermination(30, TimeUnit.SECONDS);
    channel.awaitTermination(30, TimeUnit.SECONDS);
  }

  private static class ContextInterceptor implements ServerInterceptor {
    private String role;

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      role = AUTHENTICATION_ROLE_CTX_KEY.get();
      return next.startCall(call, headers);
    }

    public String getRole() {
      return role;
    }
  }

  @Test
  public void testAuthenticationBasic() throws IOException {
    AuthenticationProvider provider = new AuthenticationProviderBasic();
    provider.initialize(null);
    doReturn(provider)
        .when(authenticationService)
        .getAuthenticationProvider(provider.getAuthMethodName());

    Metadata headers = new Metadata();
    String authorization =
        "Basic " + Base64.getEncoder().encodeToString(("superUser:supepass").getBytes());
    headers.put(AUTHORIZATION_METADATA_KEY, authorization);
    headers.put(AUTH_METHOD_METADATA_KEY, "basic");

    PulsarGrpc.PulsarBlockingStub pulsarStub = MetadataUtils.attachHeaders(this.stub, headers);
    pulsarStub.ping(PING);

    assertEquals(contextInterceptor.getRole(), "superUser");
  }

  @Test
  public void testAuthenticationToken() throws IOException {
    SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    ServiceConfiguration config = new ServiceConfiguration();
    config
        .getProperties()
        .setProperty(
            "tokenSecretKey",
            "data:;base64," + Base64.getEncoder().encodeToString(secretKey.getEncoded()));

    AuthenticationProvider provider = new AuthenticationProviderToken();
    provider.initialize(config);
    doReturn(provider)
        .when(authenticationService)
        .getAuthenticationProvider(provider.getAuthMethodName());

    String clientRole = "client";
    String clientToken = Jwts.builder().setSubject(clientRole).signWith(secretKey).compact();

    Metadata headers = new Metadata();
    headers.put(AUTHORIZATION_METADATA_KEY, "Bearer " + clientToken);
    headers.put(AUTH_METHOD_METADATA_KEY, "token");

    PulsarGrpc.PulsarBlockingStub pulsarStub = MetadataUtils.attachHeaders(this.stub, headers);
    pulsarStub.ping(PING);

    assertEquals(contextInterceptor.getRole(), clientRole);
  }

  @Test
  public void testAuthenticationTLS() throws Exception {
    AuthenticationProvider provider = new AuthenticationProviderTls();
    provider.initialize(null);
    doReturn(provider)
        .when(authenticationService)
        .getAuthenticationProvider(provider.getAuthMethodName());

    Metadata headers = new Metadata();
    headers.put(AUTH_METHOD_METADATA_KEY, "tls");

    PulsarGrpc.PulsarBlockingStub pulsarStub = MetadataUtils.attachHeaders(this.stub, headers);
    pulsarStub.ping(PING);

    assertEquals(contextInterceptor.getRole(), "superUser");
  }

  @Test
  public void testAuthenticationAnonymous() throws Exception {
    doThrow(AuthenticationException.class)
        .when(authenticationService)
        .authenticate(any(), anyString());
    doReturn(Optional.of("anonymous")).when(authenticationService).getAnonymousUserRole();
    stub.ping(PING);

    assertEquals(contextInterceptor.getRole(), "anonymous");
  }

  @Test
  public void testAuthenticationNoAnonymousUser() throws Exception {
    doThrow(AuthenticationException.class)
        .when(authenticationService)
        .authenticate(any(), anyString());

    try {
      stub.ping(PING);
      fail("Should have thrown UNAUTHENTICATED exception");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
    }
  }

  @Test
  public void testAuthenticationAny() throws Exception {
    doThrow(AuthenticationException.class)
        .when(authenticationService)
        .authenticate(any(), not(eq("basic")));
    doReturn("superUser").when(authenticationService).authenticate(any(), eq("basic"));
    stub.ping(PING);

    assertEquals(contextInterceptor.getRole(), "superUser");
  }

  @Test
  public void testAuthenticationInvalidProvider() {
    doReturn(null).when(authenticationService).getAuthenticationProvider("tls");

    Metadata headers = new Metadata();
    headers.put(AUTH_METHOD_METADATA_KEY, "tls");

    PulsarGrpc.PulsarBlockingStub pulsarStub = MetadataUtils.attachHeaders(this.stub, headers);

    try {
      pulsarStub.ping(PING);
      fail("Should have thrown UNAUTHENTICATED exception");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
    }
  }

  @Test
  public void testAuthenticationInvalidCredentials() throws Exception {
    AuthenticationProvider provider = new AuthenticationProviderBasic();
    provider.initialize(null);
    doReturn(provider)
        .when(authenticationService)
        .getAuthenticationProvider(provider.getAuthMethodName());

    Metadata headers = new Metadata();
    String authorization =
        "Basic " + Base64.getEncoder().encodeToString(("superUser:invalid").getBytes());
    headers.put(AUTHORIZATION_METADATA_KEY, authorization);
    headers.put(AUTH_METHOD_METADATA_KEY, "basic");

    PulsarGrpc.PulsarBlockingStub pulsarStub = MetadataUtils.attachHeaders(this.stub, headers);

    try {
      pulsarStub.ping(PING);
      fail("Should have thrown UNAUTHENTICATED exception");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
    }
  }
}
