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

package org.apache.giraph.examples.closeness;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class ClosenessEstimation extends BasicComputation<IntWritable,
    ClosenessState, NullWritable, HyperLogLogCounter> {

  @Override
  public void compute(
      Vertex<IntWritable, ClosenessState, NullWritable> vertex,
      Iterable<HyperLogLogCounter> neighborCounters) throws IOException {

    HyperLogLogCounter counter = vertex.getValue().counter();

    if (getSuperstep() == 0L) {

      counter.observe(vertex.getId().get());
      sendMessageToAllEdges(vertex, counter);

    } else {

      long numSeenBefore = counter.count();

      for (HyperLogLogCounter neighborCounter : neighborCounters) {
        counter.merge(neighborCounter);
      }

      long numNewlySeen = counter.count() - numSeenBefore;

      if (numNewlySeen > 0) {
        vertex.getValue().setHops((int) getSuperstep());
        sendMessageToAllEdges(vertex, counter);
      }
    }

    vertex.voteToHalt();
  }

}
