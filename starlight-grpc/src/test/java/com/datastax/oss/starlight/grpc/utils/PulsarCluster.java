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
package com.datastax.oss.starlight.grpc.utils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfo;

/** Pulsar cluster. */
public class PulsarCluster implements AutoCloseable {
  private final PulsarService service;
  private final BookKeeperCluster bookKeeperCluster;

  public PulsarCluster(Path tempDir) throws Exception {
    this(tempDir, new ServiceConfiguration());
  }

  public PulsarCluster(Path tempDir, ServiceConfiguration config) throws Exception {
    this.bookKeeperCluster = new BookKeeperCluster(tempDir, PortManager.nextFreePort());
    config.setZookeeperServers(bookKeeperCluster.getZooKeeperAddress());
    config.setConfigurationStoreServers(bookKeeperCluster.getZooKeeperAddress());
    config.setAdvertisedAddress("localhost");
    config.setClusterName("localhost");
    config.setManagedLedgerDefaultEnsembleSize(1);
    config.setManagedLedgerDefaultWriteQuorum(1);
    config.setManagedLedgerDefaultAckQuorum(1);
    config.setBrokerServicePort(Optional.of(PortManager.nextFreePort()));
    config.setAllowAutoTopicCreation(true);
    config.setWebSocketServiceEnabled(false);
    config.setSystemTopicEnabled(true);
    config.setBookkeeperNumberOfChannelsPerBookie(1);
    config.setBookkeeperExplicitLacIntervalInMills(500);
    config.setTransactionCoordinatorEnabled(true);
    config.setBookkeeperMetadataServiceUri(bookKeeperCluster.getBookKeeperMetadataURI());
    config.setWebServicePort(Optional.of(PortManager.nextFreePort()));
    config.setBookkeeperUseV2WireProtocol(false);
    service = new PulsarService(config);
  }

  public PulsarService getService() {
    return service;
  }

  public String getAddress() {
    return service.getWebServiceAddress();
  }

  public ClusterData getClusterData() {
    return ClusterDataImpl.builder()
        .serviceUrl(service.getWebServiceAddress())
        .serviceUrlTls(service.getWebServiceAddressTls())
        .brokerServiceUrl(service.getBrokerServiceUrl())
        .brokerServiceUrlTls(service.getBrokerServiceUrlTls())
        .build();
  }

  public void start() throws Exception {
    bookKeeperCluster.startBookie();
    service.start();
    service.getAdminClient().clusters().createCluster("localhost", ClusterData.builder().build());
    service
        .getAdminClient()
        .tenants()
        .createTenant(
            "public",
            TenantInfo.builder()
                .adminRoles(Collections.singleton("admin"))
                .allowedClusters(Collections.singleton("localhost"))
                .build());
    service.getAdminClient().namespaces().createNamespace("public/default");
  }

  public void close() throws Exception {
    service.close();
    bookKeeperCluster.close();
  }
}
