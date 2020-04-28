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
package org.apache.kafka.clients.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;


/**
 * The partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>Otherwise choose the sticky partition that changes when the batch is full.
 * 
 * NOTE: In constrast to the DefaultPartitioner, the record key is NOT used as part of the partitioning strategy in this 
 *       partitioner. Records with the same key are not guaranteed to be sent to the same partition.
 * 
 * See KIP-480 for details about sticky partitioning.
 *
 * 为了解决小包问题，使用了批处理；而批处理的阈值，在吞吐量低时比较难达到（分区越多，越难达到），这势必会导致延迟。
 * 所以这里使用了粘性分区，将消息全部放到一个分区，等创建新批次的时候，再选择下一个分区
 *
 * 具体实践：https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner
 * 优化之后，cpu 使用率降低了，而且延迟也降低了。
 *
 * 可借鉴的设计：
 *    * 批处理（解决小包问题）
 *    * 粘性（解决低吞吐导致的延时问题）
 *    * 切换（解决分区消息不均衡的问题）
 */
public class UniformStickyPartitioner implements Partitioner {

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return stickyPartitionCache.partition(topic, cluster);
    }

    public void close() {}
    
    /**
     * If a batch completed for the current sticky partition, change the sticky partition. 
     * Alternately, if no sticky partition has been determined, set one.
     */
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
    }
}
