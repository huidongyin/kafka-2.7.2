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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.*;

public class ProducerMetadata extends Metadata {
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    //控制主题元数据在多长时间内没有访问后将被从缓存中删除。默认情况下是 5 分钟。这个时间用于确定缓存中的主题元数据是否过期，并决定是否需要从集群中重新获取主题的元数据信息。
    private final long metadataIdleMs;

    /* 存储了带有过期时间的主题。这是一个 `Map` 类型的数据结构，将主题名与其对应的过期时间进行了关联。 */
    private final Map<String, Long> topics = new HashMap<>();
    //用于存储新添加的主题集合。这个集合中存储了当前生产者实例中新添加的主题。
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;

    public ProducerMetadata(long refreshBackoffMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        super(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    //构建获取元数据的请求
    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    //构建获取新主题元数据的请求
    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    //往元数据添加一个topic
    public synchronized void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        //将topic存放到带有过期时间的topic集合，如果返回结果为空，就表示是第一次添加这个topic
        //此时将这个topic放到新topic集合，并发起更新新topic的请求（这里没有实际发，只是修改状态位）。
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            newTopics.add(topic);
            requestUpdateForNewTopics();
        }
    }

    //请求更新topic
    public synchronized int requestUpdateForTopic(String topic) {
        //如果这个topic在新topic列表，那就走请求更新新topic的逻辑，否则走更新topic的逻辑
        if (newTopics.contains(topic)) {
            return requestUpdateForNewTopics();
        } else {
            return requestUpdate();
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    // Visible for testing
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    //判断元数据中是否应该保留该主题
    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        //从带过期时间的主题列表中获取该topic
        Long expireMs = topics.get(topic);
        //如果过期时间为空，说明不在topic列表，直接返回false。
        //如果在新的topic集合中，返回true。
        //如果元数据已经过期了，那就从topic过期列表移除这个topic，返回false。
        //否则返回true。
        if (expireMs == null) {
            return false;
        } else if (newTopics.contains(topic)) {
            return true;
        } else if (expireMs <= nowMs) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            topics.remove(topic);
            return false;
        } else {
            return true;
        }
    }

    //阻塞等待更新元数据信息的线程
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        //获取当前时间
        long currentTimeMs = time.milliseconds();
        //截止时间 = 当前时间+ 超时时间 <0 取Long的最大值，否则取当前时间+ 超时时间
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        //尝试阻塞等待，这里的time是SystemTime
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        //调用父类的update方法
        super.update(requestVersion, response, isPartialUpdate, nowMs);

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        //如果新topic集合不为空
        if (!newTopics.isEmpty()) {
            //遍历新topic集合，如果新topic出现在元数据请求的响应数据里面了，那就说明他不算新topic了，直接移除掉。
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }
        //唤醒等待元数据的所有业务线程。
        notifyAll();
    }

    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
