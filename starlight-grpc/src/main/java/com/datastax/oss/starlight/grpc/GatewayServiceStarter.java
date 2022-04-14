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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.resources.PulsarResources.createMetadataStore;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationMetadataCacheService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts an instance of Starlight for gRPC */
public class GatewayServiceStarter {

  @Parameter(
    names = {"-c", "--config"},
    description = "Configuration file path",
    required = true
  )
  private String configFile = null;

  @Parameter(
    names = {"-h", "--help"},
    description = "Show this help message"
  )
  private boolean help = false;

  public GatewayServiceStarter(String[] args) throws Exception {

    try {

      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");

      Thread.setDefaultUncaughtExceptionHandler(
          (thread, exception) ->
              System.out.printf(
                  "%s [%s] error Uncaught exception in thread %s: %s%n",
                  dateFormat.format(new Date()),
                  thread.getContextClassLoader(),
                  thread.getName(),
                  exception.getMessage()));

      JCommander jcommander = new JCommander();
      try {
        jcommander.addObject(this);
        jcommander.parse(args);
        if (help || isBlank(configFile)) {
          jcommander.usage();
          return;
        }
      } catch (Exception e) {
        jcommander.usage();
        System.exit(-1);
      }

      // load config file
      final GatewayConfiguration config =
          ConfigurationUtils.create(configFile, GatewayConfiguration.class);

      AuthenticationService authenticationService =
          new AuthenticationService(PulsarConfigurationLoader.convertFrom(config));

      MetadataStoreExtended configMetadataStore;
      ConfigurationMetadataCacheService configurationCacheService = null;
      PulsarResources pulsarResources = null;
      if (isNotBlank(config.getConfigurationStoreServers())) {
        try {
          configMetadataStore =
              createMetadataStore(
                  config.getConfigurationStoreServers(), config.getZookeeperSessionTimeoutMs());
        } catch (MetadataStoreException e) {
          throw new PulsarServerException(e);
        }
        pulsarResources = new PulsarResources(null, configMetadataStore);
        configurationCacheService = new ConfigurationMetadataCacheService(pulsarResources, null);
      } else {
        configMetadataStore = null;
      }

      AuthorizationService authorizationService = null;
      if (config.isAuthorizationEnabled()) {
        if (configurationCacheService == null) {
          throw new PulsarServerException(
              "Failed to initialize authorization manager due to empty ConfigurationStoreServers");
        }
        authorizationService =
            new AuthorizationService(
                ConfigurationUtils.convertFrom(config), configurationCacheService);
      }

      // create gateway service
      ClusterData clusterData = createClusterData(config);
      GatewayService gatewayService;
      if (clusterData != null) {
        gatewayService =
            new GatewayService(config, authenticationService, authorizationService, clusterData);
      } else {
        gatewayService =
            new GatewayService(
                config, authenticationService, authorizationService, pulsarResources);
      }

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      gatewayService.close();
                      if (configMetadataStore != null) {
                        try {
                          configMetadataStore.close();
                        } catch (Exception e) {
                          throw new IOException(e);
                        }
                      }
                    } catch (Exception e) {
                      log.warn("server couldn't stop gracefully {}", e.getMessage(), e);
                    }
                  }));

      gatewayService.start();
      gatewayService.awaitTermination();

    } catch (InterruptedException e) {
      log.info("Exit server " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("Failed to start Starlight-for-gRPC. error msg " + e.getMessage(), e);
      throw new PulsarServerException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    new GatewayServiceStarter(args);
  }

  private static ClusterData createClusterData(GatewayConfiguration config) {
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

  private static final Logger log = LoggerFactory.getLogger(GatewayServiceStarter.class);
}
