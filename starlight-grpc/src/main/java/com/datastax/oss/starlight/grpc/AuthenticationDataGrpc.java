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

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

public class AuthenticationDataGrpc implements AuthenticationDataSource {

  private final SocketAddress remoteAddress;
  private final SSLSession sslSession;
  private final Metadata metadata;

  public AuthenticationDataGrpc(
      SocketAddress remoteAddress, SSLSession sslSession, Metadata metadata) {
    this.remoteAddress = remoteAddress;
    this.sslSession = sslSession;
    this.metadata = metadata;
  }

  @Override
  public boolean hasDataFromTls() {
    return (sslSession != null);
  }

  @Override
  public Certificate[] getTlsCertificates() {
    try {
      return sslSession.getPeerCertificates();
    } catch (SSLPeerUnverifiedException e) {
      return null;
    }
  }

  @Override
  public boolean hasDataFromHttp() {
    return true;
  }

  @Override
  public String getHttpHeader(String name) {
    return metadata.get(Metadata.Key.of(name, ASCII_STRING_MARSHALLER));
  }

  @Override
  public boolean hasDataFromPeer() {
    return remoteAddress != null;
  }

  @Override
  public SocketAddress getPeerAddress() {
    return remoteAddress;
  }
}
