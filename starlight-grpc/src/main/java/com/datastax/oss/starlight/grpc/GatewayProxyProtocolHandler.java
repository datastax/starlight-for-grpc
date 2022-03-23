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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.proxy.extensions.ProxyExtension;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

public class GatewayProxyProtocolHandler implements ProxyExtension {

  private static final String PROTOCOL_NAME = "grpc";

  private GatewayConfiguration config;
  private GatewayService gatewayService;
  private MetadataStoreExtended configurationMetadataStore;

  @Override
  public String extensionName() {
    return PROTOCOL_NAME;
  }

  @Override
  public boolean accept(String protocol) {
    return PROTOCOL_NAME.equals(protocol);
  }

  @SneakyThrows
  @Override
  public void initialize(ProxyConfiguration conf) {
    config = ConfigurationUtils.create(conf.getProperties(), GatewayConfiguration.class);
  }

  @SneakyThrows
  @Override
  public void start(ProxyService proxyService) {
    ClusterData localCluster = createClusterData(config);
    if (localCluster != null) {
      gatewayService =
          new GatewayService(
              config,
              proxyService.getAuthenticationService(),
              proxyService.getAuthorizationService(),
              localCluster);
    } else {
      configurationMetadataStore = proxyService.createConfigurationMetadataStore();
      gatewayService =
          new GatewayService(
              config,
              proxyService.getAuthenticationService(),
              proxyService.getAuthorizationService(),
              new PulsarResources(null, configurationMetadataStore));
    }
    gatewayService.start();
  }

  @Override
  public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
    return Collections.emptyMap();
  }

  @SneakyThrows
  @Override
  public void close() {
    if (gatewayService != null) {
      gatewayService.close();
    }
    if (configurationMetadataStore != null) {
      configurationMetadataStore.close();
    }
  }

  private static ClusterData createClusterData(ProxyConfiguration config) {
    if (isNotBlank(config.getBrokerServiceURL()) || isNotBlank(config.getBrokerServiceURLTLS())) {
      return ClusterData.builder()
          .serviceUrl(config.getBrokerWebServiceURL())
          .serviceUrlTls(config.getBrokerWebServiceURLTLS())
          .brokerServiceUrl(config.getBrokerServiceURL())
          .brokerServiceUrlTls(config.getBrokerServiceURLTLS())
          .build();
    } else if (isNotBlank(config.getBrokerWebServiceURL())
        || isNotBlank(config.getBrokerWebServiceURLTLS())) {
      return ClusterData.builder()
          .serviceUrl(config.getBrokerWebServiceURL())
          .serviceUrlTls(config.getBrokerWebServiceURLTLS())
          .build();
    } else {
      return null;
    }
  }
}
