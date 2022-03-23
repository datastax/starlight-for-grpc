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

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.proxy.server.ProxyConfiguration;

@Getter
@Setter
public class GatewayConfiguration extends ProxyConfiguration {
  private static final String CATEGORY_GRPC = "gRPC";

  @FieldContext(category = CATEGORY_GRPC, doc = "gRPC service port")
  private Integer grpcServicePort;

  @FieldContext(category = CATEGORY_GRPC, doc = "gRPC TLS servicePort")
  private Integer grpcServicePortTls;

  @FieldContext(
    category = CATEGORY_GRPC,
    doc = "Number of IO threads in Pulsar Client used in gRPC proxy"
  )
  private int grpcSocketNumIoThreads = Runtime.getRuntime().availableProcessors();

  @FieldContext(
    category = CATEGORY_GRPC,
    doc = "Number of connections per Broker in Pulsar Client used in gRPC proxy"
  )
  private int grpcSocketConnectionsPerBroker = Runtime.getRuntime().availableProcessors();

  @FieldContext(
    category = CATEGORY_GRPC,
    doc =
        "If set, the gRPC service will use these parameters to authenticate on Pulsar's brokers. "
            + "If not set, the brokerClientAuthenticationParameters setting will be used. "
            + "This setting allows to have different credentials for the proxy and for the gRPC service"
  )
  private String grpcBrokerClientAuthenticationParameters;
}
