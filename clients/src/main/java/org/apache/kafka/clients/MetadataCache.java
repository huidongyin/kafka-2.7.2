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
package org.apache.kafka.clients;

import org.apache.kafka.common.*;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 */
public class MetadataCache {
    //集群id
    private final String clusterId;
    //brokerID 和 broker节点：{id,host,port ,rack}
    private final Map<Integer, Node> nodes;
    //未授权的topic列表
    private final Set<String> unauthorizedTopics;
    //无效的topic列表
    private final Set<String> invalidTopics;
    //内部topic列表
    private final Set<String> internalTopics;
    //控制器节点
    private final Node controller;
    //TopicPartition: {topicName,partitionId} : PartitionMetadata:{TopicPartition,异常信息，leader副本所在的brokerId，leader的纪元信息，分区的AR集合，分区的ISR集合，分区的OSR集合}
    private final Map<TopicPartition, PartitionMetadata> metadataByPartition;

    //集群元数据信息
    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  Map<Integer, Node> nodes,
                  Collection<PartitionMetadata> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    private MetadataCache(String clusterId,
                          Map<Integer, Node> nodes,
                          Collection<PartitionMetadata> partitions,
                          Set<String> unauthorizedTopics,
                          Set<String> invalidTopics,
                          Set<String> internalTopics,
                          Node controller,
                          Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = nodes;
        this.unauthorizedTopics = unauthorizedTopics;
        this.invalidTopics = invalidTopics;
        this.internalTopics = internalTopics;
        this.controller = controller;

        this.metadataByPartition = new HashMap<>(partitions.size());
        for (PartitionMetadata p : partitions) {
            this.metadataByPartition.put(p.topicPartition, p);
        }

        //初始化的时候这里其实不为空，不会走到这里。
        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    /**
     * 从缓存中获取指定主题分区的Partition元信息
     * @param topicPartition
     * @return
     */
    Optional<PartitionMetadata> partitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition));
    }

    /**
     * 根据broker的节点id获取broker节点{id,host,port,rack}
     * @param id
     * @return
     */
    Optional<Node> nodeById(int id) {
        return Optional.ofNullable(nodes.get(id));
    }

    /**
     * 返回集群元信息
     * @return
     */
    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    /**
     * 创建ClusterResource，这个类的意义就是存储和查询集群的唯一ID。
     * @return
     */
    ClusterResource clusterResource() {
        return new ClusterResource(clusterId);
    }

    //将新的元数据信息和当前的元数据信息合并，并返回一个新的MetadataCache对象。
    MetadataCache mergeWith(String newClusterId,
                            Map<Integer, Node> newNodes,
                            Collection<PartitionMetadata> addPartitions,
                            Set<String> addUnauthorizedTopics,
                            Set<String> addInvalidTopics,
                            Set<String> addInternalTopics,
                            Node newController,
                            //决定是否需要保留老的数据
                            BiPredicate<String, Boolean> retainTopic) {
        //创建一个Predicate用于确定是否应该保留特定的主题，shouldRetainTopic的逻辑由retainTopic函数和internalTopics集合决定。
        Predicate<String> shouldRetainTopic = topic -> retainTopic.test(topic, internalTopics.contains(topic));
        //创建一个新的空集合，用来存储合并后的元数据信息。
        Map<TopicPartition, PartitionMetadata> newMetadataByPartition = new HashMap<>(addPartitions.size());
        //将要添加的分区元数据信息加入到newMetadataByPartition集合中。
        for (PartitionMetadata partition : addPartitions) {
            newMetadataByPartition.put(partition.topicPartition, partition);
        }
        //遍历已有的分区元数据信息，根据shouldRetainTopic条件，将需要保留的分区信息加入到newMetadataByPartition中。
        for (Map.Entry<TopicPartition, PartitionMetadata> entry : metadataByPartition.entrySet()) {
            if (shouldRetainTopic.test(entry.getKey().topic())) {
                newMetadataByPartition.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        //将新的未授权的主题信息和现有的未授权的主题信息进行合并，根据shouldRetainTopic确定哪些主题需要被保留。
        Set<String> newUnauthorizedTopics = fillSet(addUnauthorizedTopics, unauthorizedTopics, shouldRetainTopic);
        //同上，合并无效的主题信息列表
        Set<String> newInvalidTopics = fillSet(addInvalidTopics, invalidTopics, shouldRetainTopic);
        //同上，合并内部的主题信息列表
        Set<String> newInternalTopics = fillSet(addInternalTopics, internalTopics, shouldRetainTopic);
        //返回一个新的MetadataCache对象，包含了合并后的信息。
        return new MetadataCache(newClusterId, newNodes, newMetadataByPartition.values(), newUnauthorizedTopics,
                newInvalidTopics, newInternalTopics, newController);
    }

    //创建一个空集合，根据断言结果决定是否要将数据加入这个空集合
    private static <T> Set<T> fillSet(Set<T> baseSet, Set<T> fillSet, Predicate<T> predicate) {
        Set<T> result = new HashSet<>(baseSet);
        for (T element : fillSet) {
            if (predicate.test(element)) {
                result.add(element);
            }
        }
        return result;
    }

    //根据当前对象的分区元数据信息，和其他元数据信息，构建集群元数据信息。
    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(metadata -> MetadataResponse.toPartitionInfo(metadata, nodes))
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes.values(), partitionInfos, unauthorizedTopics,
                invalidTopics, internalTopics, controller);
    }

    //初始化MetadataCache
    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        Map<Integer, Node> nodes = new HashMap<>();
        int nodeId = -1;
        //初始化brokerId和broker信息的映射表，这里因为此时不知道具体的brokerId，所以以负数表示brokerId。
        for (InetSocketAddress address : addresses) {
            nodes.put(nodeId, new Node(nodeId, address.getHostString(), address.getPort()));
            nodeId--;
        }
        //调用MetadataCache的构造函数，并初始化Cluster信息。
        return new MetadataCache(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                null, Cluster.bootstrap(addresses));
    }

    //初始化空的MetadataCache
    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyMap(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "clusterId='" + clusterId + '\'' +
                ", nodes=" + nodes +
                ", partitions=" + metadataByPartition.values() +
                ", controller=" + controller +
                '}';
    }

}
