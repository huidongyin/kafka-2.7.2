/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopicAdminTest {

    /**
     * 0.11.0.0 clients can talk with older brokers, but the CREATE_TOPIC API was added in 0.10.1.0. That means,
     * if our TopicAdmin talks to a pre 0.10.1 broker, it should receive an UnsupportedVersionException, should
     * create no topics, and return false.
     */
    @Test
    public void returnEmptyWithApiVersionMismatchOnCreate() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(createTopicResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            assertTrue(admin.createOrFindTopics(newTopic).isEmpty());
        }
    }

    /**
     * 0.11.0.0 clients can talk with older brokers, but the DESCRIBE_TOPIC API was added in 0.10.0.0. That means,
     * if our TopicAdmin talks to a pre 0.10.0 broker, it should receive an UnsupportedVersionException, should
     * create no topics, and return false.
     */
    @Test
    public void throwsWithApiVersionMismatchOnDescribe() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(describeTopicResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Exception e = assertThrows(ConnectException.class, () -> admin.describeTopics(newTopic.name()));
            assertTrue(e.getCause() instanceof UnsupportedVersionException);
        }
    }

    @Test
    public void returnEmptyWithClusterAuthorizationFailureOnCreate() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(createTopicResponseWithClusterAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            assertFalse(admin.createTopic(newTopic));

            env.kafkaClient().prepareResponse(createTopicResponseWithClusterAuthorizationException(newTopic));
            assertTrue(admin.createOrFindTopics(newTopic).isEmpty());
        }
    }

    @Test
    public void throwsWithClusterAuthorizationFailureOnDescribe() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeTopicResponseWithClusterAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Exception e = assertThrows(ConnectException.class, () -> admin.describeTopics(newTopic.name()));
            assertTrue(e.getCause() instanceof ClusterAuthorizationException);
        }
    }

    @Test
    public void returnEmptyWithTopicAuthorizationFailureOnCreate() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(createTopicResponseWithTopicAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            assertFalse(admin.createTopic(newTopic));

            env.kafkaClient().prepareResponse(createTopicResponseWithTopicAuthorizationException(newTopic));
            assertTrue(admin.createOrFindTopics(newTopic).isEmpty());
        }
    }

    @Test
    public void throwsWithTopicAuthorizationFailureOnDescribe() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeTopicResponseWithTopicAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Exception e = assertThrows(ConnectException.class, () -> admin.describeTopics(newTopic.name()));
            assertTrue(e.getCause() instanceof TopicAuthorizationException);
        }
    }

    @Test
    public void shouldNotCreateTopicWhenItAlreadyExists() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, "myTopic", Collections.singletonList(topicPartitionInfo), null);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            assertFalse(admin.createTopic(newTopic));
            assertTrue(admin.createTopics(newTopic).isEmpty());
            assertTrue(admin.createOrFindTopic(newTopic));
            TopicAdmin.TopicCreationResponse response = admin.createOrFindTopics(newTopic);
            assertTrue(response.isCreatedOrExisting(newTopic.name()));
            assertTrue(response.isExisting(newTopic.name()));
            assertFalse(response.isCreated(newTopic.name()));
        }
    }

    @Test
    public void shouldCreateTopicWithPartitionsWhenItDoesNotExist() {
        for (int numBrokers = 1; numBrokers < 10; ++numBrokers) {
            int expectedReplicas = Math.min(3, numBrokers);
            int maxDefaultRf = Math.min(numBrokers, 5);
            for (int numPartitions = 1; numPartitions < 30; ++numPartitions) {
                NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(numPartitions).compacted().build();

                // Try clusters with no default replication factor or default partitions
                assertTopicCreation(numBrokers, newTopic, null, null, expectedReplicas, numPartitions);

                // Try clusters with different default partitions
                for (int defaultPartitions = 1; defaultPartitions < 20; ++defaultPartitions) {
                    assertTopicCreation(numBrokers, newTopic, defaultPartitions, null, expectedReplicas, numPartitions);
                }

                // Try clusters with different default replication factors
                for (int defaultRF = 1; defaultRF < maxDefaultRf; ++defaultRF) {
                    assertTopicCreation(numBrokers, newTopic, null, defaultRF, defaultRF, numPartitions);
                }
            }
        }
    }

    @Test
    public void shouldCreateTopicWithReplicationFactorWhenItDoesNotExist() {
        for (int numBrokers = 1; numBrokers < 10; ++numBrokers) {
            int maxRf = Math.min(numBrokers, 5);
            int maxDefaultRf = Math.min(numBrokers, 5);
            for (short rf = 1; rf < maxRf; ++rf) {
                NewTopic newTopic = TopicAdmin.defineTopic("myTopic").replicationFactor(rf).compacted().build();

                // Try clusters with no default replication factor or default partitions
                assertTopicCreation(numBrokers, newTopic, null, null, rf, 1);

                // Try clusters with different default partitions
                for (int numPartitions = 1; numPartitions < 30; ++numPartitions) {
                    assertTopicCreation(numBrokers, newTopic, numPartitions, null, rf, numPartitions);
                }

                // Try clusters with different default replication factors
                for (int defaultRF = 1; defaultRF < maxDefaultRf; ++defaultRF) {
                    assertTopicCreation(numBrokers, newTopic, null, defaultRF, rf, 1);
                }
            }
        }
    }

    @Test
    public void shouldCreateTopicWithDefaultPartitionsAndReplicationFactorWhenItDoesNotExist() {
        NewTopic newTopic = TopicAdmin.defineTopic("my-topic")
                                      .defaultPartitions()
                                      .defaultReplicationFactor()
                                      .compacted()
                                      .build();

        for (int numBrokers = 1; numBrokers < 10; ++numBrokers) {
            int expectedReplicas = Math.min(3, numBrokers);
            assertTopicCreation(numBrokers, newTopic, null, null, expectedReplicas, 1);
            assertTopicCreation(numBrokers, newTopic, 30, null, expectedReplicas, 30);
        }
    }

    @Test
    public void shouldCreateOneTopicWhenProvidedMultipleDefinitionsWithSameTopicName() {
        NewTopic newTopic1 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        NewTopic newTopic2 = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (TopicAdmin admin = new TopicAdmin(null, new MockAdminClient(cluster.nodes(), cluster.nodeById(0)))) {
            Set<String> newTopicNames = admin.createTopics(newTopic1, newTopic2);
            assertEquals(1, newTopicNames.size());
            assertEquals(newTopic2.name(), newTopicNames.iterator().next());
        }
    }

    @Test
    public void createShouldReturnFalseWhenSuppliedNullTopicDescription() {
        Cluster cluster = createCluster(1);
        try (TopicAdmin admin = new TopicAdmin(null, new MockAdminClient(cluster.nodes(), cluster.nodeById(0)))) {
            boolean created = admin.createTopic(null);
            assertFalse(created);
        }
    }

    @Test
    public void describeShouldReturnEmptyWhenTopicDoesNotExist() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (TopicAdmin admin = new TopicAdmin(null, new MockAdminClient(cluster.nodes(), cluster.nodeById(0)))) {
            assertTrue(admin.describeTopics(newTopic.name()).isEmpty());
        }
    }

    @Test
    public void describeShouldReturnTopicDescriptionWhenTopicExists() {
        String topicName = "myTopic";
        NewTopic newTopic = TopicAdmin.defineTopic(topicName).partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, topicName, Collections.singletonList(topicPartitionInfo), null);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            Map<String, TopicDescription> desc = admin.describeTopics(newTopic.name());
            assertFalse(desc.isEmpty());
            TopicDescription topicDesc = new TopicDescription(topicName, false, Collections.singletonList(topicPartitionInfo));
            assertEquals(desc.get("myTopic"), topicDesc);
        }
    }

    @Test
    public void describeTopicConfigShouldReturnEmptyMapWhenNoTopicsAreSpecified() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeConfigsResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Map<String, Config> results = admin.describeTopicConfigs();
            assertTrue(results.isEmpty());
        }
    }

    @Test
    public void describeTopicConfigShouldReturnEmptyMapWhenUnsupportedVersionFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeConfigsResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Map<String, Config> results = admin.describeTopicConfigs(newTopic.name());
            assertTrue(results.isEmpty());
        }
    }

    @Test
    public void describeTopicConfigShouldReturnEmptyMapWhenClusterAuthorizationFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeConfigsResponseWithClusterAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Map<String, Config> results = admin.describeTopicConfigs(newTopic.name());
            assertTrue(results.isEmpty());
        }
    }

    @Test
    public void describeTopicConfigShouldReturnEmptyMapWhenTopicAuthorizationFailure() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeConfigsResponseWithTopicAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            Map<String, Config> results = admin.describeTopicConfigs(newTopic.name());
            assertTrue(results.isEmpty());
        }
    }

    @Test
    public void describeTopicConfigShouldReturnMapWithNullValueWhenTopicDoesNotExist() {
        NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (TopicAdmin admin = new TopicAdmin(null, new MockAdminClient(cluster.nodes(), cluster.nodeById(0)))) {
            Map<String, Config> results = admin.describeTopicConfigs(newTopic.name());
            assertFalse(results.isEmpty());
            assertEquals(1, results.size());
            assertNull(results.get("myTopic"));
        }
    }

    @Test
    public void describeTopicConfigShouldReturnTopicConfigWhenTopicExists() {
        String topicName = "myTopic";
        NewTopic newTopic = TopicAdmin.defineTopic(topicName)
                                      .config(Collections.singletonMap("foo", "bar"))
                                      .partitions(1)
                                      .compacted()
                                      .build();
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, topicName, Collections.singletonList(topicPartitionInfo), null);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            Map<String, Config> result = admin.describeTopicConfigs(newTopic.name());
            assertFalse(result.isEmpty());
            assertEquals(1, result.size());
            Config config = result.get("myTopic");
            assertNotNull(config);
            config.entries().forEach(entry -> {
                assertEquals(newTopic.configs().get(entry.name()), entry.value());
            });
        }
    }

    @Test
    public void verifyingTopicCleanupPolicyShouldReturnFalseWhenBrokerVersionIsUnsupported() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeConfigsResponseWithUnsupportedVersion(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean result = admin.verifyTopicCleanupPolicyOnlyCompact("myTopic", "worker.topic", "purpose");
            assertFalse(result);
        }
    }

    @Test
    public void verifyingTopicCleanupPolicyShouldReturnFalseWhenClusterAuthorizationError() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeConfigsResponseWithClusterAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean result = admin.verifyTopicCleanupPolicyOnlyCompact("myTopic", "worker.topic", "purpose");
            assertFalse(result);
        }
    }

    @Test
    public void verifyingTopicCleanupPolicyShouldReturnFalseWhenTopicAuthorizationError() {
        final NewTopic newTopic = TopicAdmin.defineTopic("myTopic").partitions(1).compacted().build();
        Cluster cluster = createCluster(1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(new MockTime(), cluster)) {
            env.kafkaClient().prepareResponse(describeConfigsResponseWithTopicAuthorizationException(newTopic));
            TopicAdmin admin = new TopicAdmin(null, env.adminClient());
            boolean result = admin.verifyTopicCleanupPolicyOnlyCompact("myTopic", "worker.topic", "purpose");
            assertFalse(result);
        }
    }

    @Test
    public void verifyingTopicCleanupPolicyShouldReturnTrueWhenTopicHasCorrectPolicy() {
        String topicName = "myTopic";
        Map<String, String> topicConfigs = Collections.singletonMap("cleanup.policy", "compact");
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, topicName, Collections.singletonList(topicPartitionInfo), topicConfigs);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            boolean result = admin.verifyTopicCleanupPolicyOnlyCompact("myTopic", "worker.topic", "purpose");
            assertTrue(result);
        }
    }

    @Test
    public void verifyingTopicCleanupPolicyShouldFailWhenTopicHasDeletePolicy() {
        String topicName = "myTopic";
        Map<String, String> topicConfigs = Collections.singletonMap("cleanup.policy", "delete");
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, topicName, Collections.singletonList(topicPartitionInfo), topicConfigs);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            ConfigException e = assertThrows(ConfigException.class, () -> {
                admin.verifyTopicCleanupPolicyOnlyCompact("myTopic", "worker.topic", "purpose");
            });
            assertTrue(e.getMessage().contains("to guarantee consistency and durability"));
        }
    }

    @Test
    public void verifyingTopicCleanupPolicyShouldFailWhenTopicHasDeleteAndCompactPolicy() {
        String topicName = "myTopic";
        Map<String, String> topicConfigs = Collections.singletonMap("cleanup.policy", "delete,compact");
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, topicName, Collections.singletonList(topicPartitionInfo), topicConfigs);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            ConfigException e = assertThrows(ConfigException.class, () -> {
                admin.verifyTopicCleanupPolicyOnlyCompact("myTopic", "worker.topic", "purpose");
            });
            assertTrue(e.getMessage().contains("to guarantee consistency and durability"));
        }
    }

    @Test
    public void verifyingGettingTopicCleanupPolicies() {
        String topicName = "myTopic";
        Map<String, String> topicConfigs = Collections.singletonMap("cleanup.policy", "compact");
        Cluster cluster = createCluster(1);
        try (MockAdminClient mockAdminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0))) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.<Node>emptyList());
            mockAdminClient.addTopic(false, topicName, Collections.singletonList(topicPartitionInfo), topicConfigs);
            TopicAdmin admin = new TopicAdmin(null, mockAdminClient);
            Set<String> policies = admin.topicCleanupPolicy("myTopic");
            assertEquals(1, policies.size());
            assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, policies.iterator().next());
        }
    }

    @SuppressWarnings("unchecked")
    private <T> KafkaFuture<T> mockFuture() {
        return (KafkaFuture<T>) mock(KafkaFuture.class);
    }

    private Admin expectAdminListOffsetsFailure(Throwable expected) throws Exception {
        // When the admin client lists offsets
        Admin mockAdmin = mock(Admin.class);
        ListOffsetsResult results = mock(ListOffsetsResult.class);
        when(mockAdmin.listOffsets(anyMap())).thenReturn(results);
        // and throws an exception via the future.get()
        ExecutionException execException = new ExecutionException(expected);
        KafkaFuture<ListOffsetsResultInfo> future = mockFuture();
        when(future.get()).thenThrow(execException);
        when(results.partitionResult(any(TopicPartition.class))).thenReturn(future);
        return mockAdmin;
    }

    private void expectOffsets(ListOffsetsResult results, TopicPartition tp, long offset) throws Exception {
        // Then return the offsets for tp2
        ListOffsetsResultInfo resultsInfo = new ListOffsetsResultInfo(offset, 0L, Optional.empty());
        KafkaFuture<ListOffsetsResultInfo> future = mockFuture();
        when(future.get()).thenReturn(resultsInfo);
        when(results.partitionResult(eq(tp))).thenReturn(future);

    }

    @Test
    public void endOffsetsShouldFailWithNonRetriableWhenAuthorizationFailureOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new AuthorizationException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        ConnectException e = assertThrows(ConnectException.class, () -> {
            admin.endOffsets(tps);
        });
        assertTrue(e.getMessage().contains("Not authorized to get the end offsets"));
    }

    @Test
    public void endOffsetsShouldFailWithUnsupportedVersionWhenVersionUnsupportedErrorOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new UnsupportedVersionException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        UnsupportedVersionException e = assertThrows(UnsupportedVersionException.class, () -> {
            admin.endOffsets(tps);
        });
    }

    @Test
    public void endOffsetsShouldFailWithTimeoutExceptionWhenTimeoutErrorOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new TimeoutException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        TimeoutException e = assertThrows(TimeoutException.class, () -> {
            admin.endOffsets(tps);
        });
    }

    @Test
    public void endOffsetsShouldFailWithNonRetriableWhenUnknownErrorOccurs() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new RuntimeException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        ConnectException e = assertThrows(ConnectException.class, () -> {
            admin.endOffsets(tps);
        });
        assertTrue(e.getMessage().contains("Error while getting end offsets for topic"));
    }

    @Test
    public void endOffsetsShouldReturnEmptyMapWhenPartitionsSetIsNull() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // Then the topic admin should return immediately
        Admin mockAdmin = mock(Admin.class);
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        Map<TopicPartition, Long> offsets = admin.endOffsets(Collections.emptySet());
        assertTrue(offsets.isEmpty());
    }

    @Test
    public void endOffsetsShouldReturnOffsetsForOnePartition() throws Exception {
        String topicName = "myTopic";
        long offset = 1L;
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets is called with one topic partition
        Admin mockAdmin = mock(Admin.class);
        ListOffsetsResult results = mock(ListOffsetsResult.class);
        when(mockAdmin.listOffsets(anyMap())).thenReturn(results);
        expectOffsets(results, tp1, offset);

        // Then the topic admin should return offsets
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        Map<TopicPartition, Long> offsets = admin.endOffsets(tps);
        assertEquals(1, offsets.size());
        assertEquals(Long.valueOf(offset), offsets.get(tp1));
    }

    @Test
    public void endOffsetsShouldReturnOffsetsForMultiplePartitions() throws Exception {
        long offset1 = 1L;
        long offset2 = 2L;
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        TopicPartition tp2 = new TopicPartition(topicName, 1);
        Set<TopicPartition> tps = new HashSet<>(Arrays.asList(tp1, tp2));

        // When the admin client lists offsets is called with one topic partition
        Admin mockAdmin = mock(Admin.class);
        ListOffsetsResult results = mock(ListOffsetsResult.class);
        when(mockAdmin.listOffsets(anyMap())).thenReturn(results);
        expectOffsets(results, tp1, offset1);
        expectOffsets(results, tp2, offset2);

        // Then the topic admin should return offsets
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        Map<TopicPartition, Long> offsets = admin.endOffsets(tps);
        assertEquals(2, offsets.size());
        assertEquals(Long.valueOf(offset1), offsets.get(tp1));
        assertEquals(Long.valueOf(offset2), offsets.get(tp2));
    }

    @Test
    public void endOffsetsShouldFailWhenAnyTopicPartitionHasError() throws Exception {
        String topicName = "myTopic";
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        Set<TopicPartition> tps = Collections.singleton(tp1);

        // When the admin client lists offsets throws an exception
        Admin mockAdmin = expectAdminListOffsetsFailure(new AuthorizationException("failed"));

        // Then the topic admin should throw exception
        TopicAdmin admin = new TopicAdmin(null, mockAdmin);
        ConnectException e = assertThrows(ConnectException.class, () -> {
            admin.endOffsets(tps);
        });
        assertTrue(e.getMessage().contains("Not authorized to get the end offsets"));
    }

    private Cluster createCluster(int numNodes) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; ++i) {
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        }
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));
        return cluster;
    }

    private CreateTopicsResponse createTopicResponseWithUnsupportedVersion(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.UNSUPPORTED_VERSION, "This version of the API is not supported"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithClusterAuthorizationException(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private CreateTopicsResponse createTopicResponseWithTopicAuthorizationException(NewTopic... topics) {
        return createTopicResponse(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private CreateTopicsResponse createTopicResponse(ApiError error, NewTopic... topics) {
        if (error == null) error = new ApiError(Errors.NONE, "");
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        for (NewTopic topic : topics) {
            response.topics().add(new CreatableTopicResult().
                setName(topic.name()).
                setErrorCode(error.error().code()).
                setErrorMessage(error.message()));
        }
        return new CreateTopicsResponse(response);
    }

    protected void assertTopicCreation(
            int brokers,
            NewTopic newTopic,
            Integer defaultPartitions,
            Integer defaultReplicationFactor,
            int expectedReplicas,
            int expectedPartitions
    ) {
        Cluster cluster = createCluster(brokers);
        MockAdminClient.Builder clientBuilder = MockAdminClient.create();
        if (defaultPartitions != null) {
            clientBuilder.defaultPartitions(defaultPartitions.shortValue());
        }
        if (defaultReplicationFactor != null) {
            clientBuilder.defaultReplicationFactor(defaultReplicationFactor.intValue());
        }
        clientBuilder.brokers(cluster.nodes());
        clientBuilder.controller(0);
        try (MockAdminClient admin = clientBuilder.build()) {
            TopicAdmin topicClient = new TopicAdmin(null, admin, false);
            TopicAdmin.TopicCreationResponse response = topicClient.createOrFindTopics(newTopic);
            assertTrue(response.isCreated(newTopic.name()));
            assertFalse(response.isExisting(newTopic.name()));
            assertTopic(admin, newTopic.name(), expectedPartitions, expectedReplicas);
        }
    }

    protected void assertTopic(MockAdminClient admin, String topicName, int expectedPartitions, int expectedReplicas) {
        TopicDescription desc = null;
        try {
            desc = topicDescription(admin, topicName);
        } catch (Throwable t) {
            fail("Failed to find topic description for topic '" + topicName + "'");
        }
        assertEquals(expectedPartitions, desc.partitions().size());
        for (TopicPartitionInfo tp : desc.partitions()) {
            assertEquals(expectedReplicas, tp.replicas().size());
        }
    }

    protected TopicDescription topicDescription(MockAdminClient admin, String topicName)
            throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = admin.describeTopics(Collections.singleton(topicName));
        Map<String, KafkaFuture<TopicDescription>> byName = result.values();
        return byName.get(topicName).get();
    }

    private MetadataResponse describeTopicResponseWithUnsupportedVersion(NewTopic... topics) {
        return describeTopicResponse(new ApiError(Errors.UNSUPPORTED_VERSION, "This version of the API is not supported"), topics);
    }

    private MetadataResponse describeTopicResponseWithClusterAuthorizationException(NewTopic... topics) {
        return describeTopicResponse(new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private MetadataResponse describeTopicResponseWithTopicAuthorizationException(NewTopic... topics) {
        return describeTopicResponse(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private MetadataResponse describeTopicResponse(ApiError error, NewTopic... topics) {
        if (error == null) error = new ApiError(Errors.NONE, "");
        MetadataResponseData response = new MetadataResponseData();
        for (NewTopic topic : topics) {
            response.topics().add(new MetadataResponseTopic()
                    .setName(topic.name())
                    .setErrorCode(error.error().code()));
        }
        return new MetadataResponse(response);
    }

    private DescribeConfigsResponse describeConfigsResponseWithUnsupportedVersion(NewTopic... topics) {
        return describeConfigsResponse(new ApiError(Errors.UNSUPPORTED_VERSION, "This version of the API is not supported"), topics);
    }

    private DescribeConfigsResponse describeConfigsResponseWithClusterAuthorizationException(NewTopic... topics) {
        return describeConfigsResponse(new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private DescribeConfigsResponse describeConfigsResponseWithTopicAuthorizationException(NewTopic... topics) {
        return describeConfigsResponse(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "Not authorized to create topic(s)"), topics);
    }

    private DescribeConfigsResponse describeConfigsResponse(ApiError error, NewTopic... topics) {
        List<DescribeConfigsResponseData.DescribeConfigsResult> results = Stream.of(topics)
                .map(topic -> new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setErrorCode(error.error().code())
                        .setErrorMessage(error.message())
                        .setResourceType(ConfigResource.Type.TOPIC.id())
                        .setResourceName(topic.name())
                        .setConfigs(topic.configs().entrySet()
                                .stream()
                                .map(e -> new DescribeConfigsResponseData.DescribeConfigsResourceResult()
                                        .setName(e.getKey())
                                        .setValue(e.getValue()))
                                .collect(Collectors.toList())))
                .collect(Collectors.toList());
        return new DescribeConfigsResponse(new DescribeConfigsResponseData().setThrottleTimeMs(1000).setResults(results));
    }

}
