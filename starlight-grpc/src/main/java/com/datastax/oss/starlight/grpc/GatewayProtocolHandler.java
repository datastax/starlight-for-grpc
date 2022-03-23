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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayProtocolHandler implements ProtocolHandler {
  private static final Logger log = LoggerFactory.getLogger(GatewayProtocolHandler.class);
  private static final String NAME = "grpc-proxy";

  private GatewayConfiguration config;
  private String advertisedAddress = null;
  private GatewayService gatewayService;

  @Override
  public String protocolName() {
    return NAME;
  }

  @Override
  public boolean accept(String protocol) {
    return NAME.equals(protocol);
  }

  @SneakyThrows
  @Override
  public void initialize(ServiceConfiguration conf) {
    config = ConfigurationUtils.create(conf.getProperties(), GatewayConfiguration.class);
  }

  @Override
  public String getProtocolDataToAdvertise() {
    List<String> args = new ArrayList<>();
    //    if (advertisedAddress != null) {
    //      StringBuilder sb = new StringBuilder(GRPC_SERVICE_HOST_PROPERTY_NAME);
    //      args.add(sb.append("=").append(advertisedAddress).toString());
    //    }
    //    if (server != null) {
    //      StringBuilder sb = new StringBuilder(GRPC_SERVICE_PORT_PROPERTY_NAME);
    //      args.add(sb.append("=").append(server.getPort()).toString());
    //    }
    //    if (tlsServer != null) {
    //      StringBuilder sb = new StringBuilder(GRPC_SERVICE_PORT_TLS_PROPERTY_NAME);
    //      args.add(sb.append("=").append(tlsServer.getPort()).toString());
    //    }
    return String.join(";", args);
  }

  @SneakyThrows
  @Override
  public void start(BrokerService service) {
    PulsarService pulsar = service.pulsar();
    ClusterDataImpl clusterData =
        ClusterDataImpl.builder()
            .serviceUrl(pulsar.getWebServiceAddress())
            .serviceUrlTls(pulsar.getWebServiceAddressTls())
            .brokerServiceUrl(pulsar.getBrokerServiceUrl())
            .brokerServiceUrlTls(pulsar.getBrokerServiceUrlTls())
            .build();
    gatewayService =
        new GatewayService(
            config,
            service.getAuthenticationService(),
            service.getAuthorizationService(),
            clusterData);
    gatewayService.start();
  }

  @Override
  public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
    // The gRPC server uses it's own Netty setup.
    return Collections.emptyMap();
  }

  @SneakyThrows
  @Override
  public void close() {
    if (gatewayService != null) {
      gatewayService.close();
    }
  }
}
