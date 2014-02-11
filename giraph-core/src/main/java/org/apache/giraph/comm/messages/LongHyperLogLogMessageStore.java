/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm.messages;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LongHyperLogLogMessageStore
    implements MessageStore<LongWritable, HyperLogLog> {

  /** Map from partition id to map from vertex id to message */
  private final Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<HyperLogLog>> map;
  /** Message messageCombiner */
  private final MessageCombiner<LongWritable, HyperLogLog> messageCombiner;
  /** Service worker */
  private final CentralizedServiceWorker<LongWritable, ?, ?> service;

  /**
   * Constructor
   *
   * @param service Service worker
   * @param messageCombiner Message messageCombiner
   */
  public LongHyperLogLogMessageStore(
      CentralizedServiceWorker<LongWritable, ?, ?> service,
      MessageCombiner<LongWritable, HyperLogLog> messageCombiner) {
    this.service = service;
    this.messageCombiner = messageCombiner;

    map = new Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<HyperLogLog>>();
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Partition<LongWritable, ?, ?> partition =
          service.getPartitionStore().getOrCreatePartition(partitionId);
      Long2ObjectOpenHashMap<HyperLogLog> partitionMap =
          new Long2ObjectOpenHashMap<HyperLogLog>(
              (int) partition.getVertexCount());
      map.put(partitionId, partitionMap);
    }
  }

  /**
   * Get map which holds messages for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds messages for partition which vertex belongs to.
   */
  private Long2ObjectOpenHashMap<HyperLogLog> getPartitionMap(
      LongWritable vertexId) {
    return map.get(service.getPartitionId(vertexId));
  }

  @Override
  public void addPartitionMessages(int partitionId,
      ByteArrayVertexIdMessages<LongWritable, HyperLogLog> messages) throws
      IOException {
    LongWritable reusableVertexId = new LongWritable();
    //HyperLogLog reusableMessage = new HyperLogLog();
    //HyperLogLog reusableCurrentMessage = new HyperLogLog();

    Long2ObjectOpenHashMap<HyperLogLog> partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      ByteArrayVertexIdMessages<LongWritable,
          HyperLogLog>.VertexIdMessageIterator iterator =
          messages.getVertexIdMessageIterator();
      while (iterator.hasNext()) {
        iterator.next();
        long vertexId = iterator.getCurrentVertexId().get();
        HyperLogLog receivedCounter = iterator.getCurrentMessage();
        if (partitionMap.containsKey(vertexId)) {
          reusableVertexId.set(vertexId);
          //reusableMessage.set(receivedCounter);
          //reusableCurrentMessage.set(partitionMap.get(vertexId));
          HyperLogLog currentCounter = partitionMap.get(vertexId);
          messageCombiner.combine(reusableVertexId, currentCounter,
              receivedCounter);
          //TODO is this call necessary?
          partitionMap.put(vertexId, currentCounter);
          //receivedCounter = reusableCurrentMessage.get();
        } else {
          partitionMap.put(vertexId, receivedCounter.clone());
        }
      }
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    map.get(partitionId).clear();
  }

  @Override
  public boolean hasMessagesForVertex(LongWritable vertexId) {
    return getPartitionMap(vertexId).containsKey(vertexId.get());
  }

  @Override
  public Iterable<HyperLogLog> getVertexMessages(
      LongWritable vertexId) throws IOException {
    Long2ObjectOpenHashMap<HyperLogLog> partitionMap =
        getPartitionMap(vertexId);
    if (!partitionMap.containsKey(vertexId.get())) {
      return EmptyIterable.get();
    } else {
      return Collections.singleton(partitionMap.get(vertexId.get()));
    }
  }

  @Override
  public void clearVertexMessages(LongWritable vertexId) throws IOException {
    getPartitionMap(vertexId).remove(vertexId.get());
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }

  @Override
  public Iterable<LongWritable> getPartitionDestinationVertices(
      int partitionId) {
    Long2ObjectOpenHashMap<HyperLogLog> partitionMap = map.get(partitionId);
    List<LongWritable> vertices =
        Lists.newArrayListWithCapacity(partitionMap.size());
    LongIterator iterator = partitionMap.keySet().iterator();
    while (iterator.hasNext()) {
      vertices.add(new LongWritable(iterator.nextLong()));
    }
    return vertices;
  }

  @Override
  public void writePartition(DataOutput out,
                             int partitionId) throws IOException {
    Long2ObjectOpenHashMap<HyperLogLog> partitionMap = map.get(partitionId);
    out.writeInt(partitionMap.size());
//    ObjectIterator<Long2DoubleMap.Entry> iterator =
//        partitionMap.long2DoubleEntrySet().fastIterator();
    ObjectIterator<Long2ObjectMap.Entry<HyperLogLog>> iterator =
        partitionMap.long2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      //Long2DoubleMap.Entry entry = iterator.next();
      Long2ObjectMap.Entry<HyperLogLog> entry = iterator.next();
      out.writeLong(entry.getLongKey());
      entry.getValue().write(out);
      //out.writeDouble(entry.getDoubleValue());
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
                                     int partitionId) throws IOException {
    int size = in.readInt();
    Long2ObjectOpenHashMap<HyperLogLog> partitionMap =
        new Long2ObjectOpenHashMap<HyperLogLog>(size);
    while (size-- > 0) {
      long vertexId = in.readLong();
      HyperLogLog counter = new HyperLogLog();
      counter.readFields(in);
      //double message = in.readDouble();
      partitionMap.put(vertexId, counter);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
